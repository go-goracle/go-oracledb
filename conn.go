// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package oracledb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"

	"github.com/go-goracle/go-oracledb/internal"
)

const getConnection = "--GET_CONNECTION--"
const wrapResultset = "--WRAP_RESULTSET--"

var _ = driver.Conn((*conn)(nil))
var _ = driver.ConnBeginTx((*conn)(nil))
var _ = driver.ConnPrepareContext((*conn)(nil))
var _ = driver.Pinger((*conn)(nil))

//var _ = driver.ExecerContext((*conn)(nil))

type (
	ConnectionParams = internal.ConnectionParams
	ObjectType       = internal.ObjectType
	TraceTag         = internal.TraceTag
)

type conn struct {
	connParams     ConnectionParams
	currentTT      TraceTag
	Client, Server internal.VersionInfo
	tranParams     tranParams
	sync.RWMutex
	currentUser string
	*drv
	iConn         *internal.Conn
	inTransaction bool
	newSession    bool
	tzOffSecs     int
	objTypes      map[string]ObjectType
}

func (c *conn) Err() error {
	if c == nil || c.drv == nil {
		return driver.ErrBadConn
	}
	return c.drv.Err()
}

func (c *conn) Break() error {
	c.RLock()
	defer c.RUnlock()
	if Log != nil {
		Log("msg", "Break", "dpiConn", c.dpiConn)
	}
	return maybeBadConn(c.iConn.Break())
}

// Ping checks the connection's state.
//
// WARNING: as database/sql calls database/sql/driver.Open when it needs
// a new connection, but does not provide this Context,
// if the Open stalls (unreachable / firewalled host), the
// database/sql.Ping may return way after the Context.Deadline!
func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.ensureContextUser(ctx); err != nil {
		return err
	}
	c.RLock()
	defer c.RUnlock()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- c.iConn.Ping()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// select again to avoid race condition if both are done
		select {
		case err := <-done:
			return err
		default:
			_ = c.Break()
			c.close(true)
			return driver.ErrBadConn
		}
	}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *conn) Close() error {
	if c == nil {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	return c.close(true)
}

func (c *conn) close(doNotReuse bool) error {
	if c == nil {
		return nil
	}
	c.iConn.SetTraceTag(TraceTag{})
	iConn, objTypes := c.iConn, c.objTypes
	c.iConn, c.objTypes = nil, nil
	if iConn == nil {
		return nil
	}
	defer iConn.Release()

	seen := make(map[string]struct{}, len(objTypes))
	for _, o := range objTypes {
		nm := o.FullName()
		if _, seen := seen[nm]; seen {
			continue
		}
		seen[nm] = struct{}{}
		o.close()
	}
	if !doNotReuse {
		return nil
	}

	// Just to be sure, break anything in progress.
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			if Log != nil {
				Log("msg", "TIMEOUT releasing connection")
			}
			iConn.Break()
		}
	}()
	iConn.Close()
	close(done)
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	const (
		trRO = "READ ONLY"
		trRW = "READ WRITE"
		trLC = "ISOLATION LEVEL READ COMMIT" + "TED" // against misspell check
		trLS = "ISOLATION LEVEL SERIALIZABLE"
	)

	var todo tranParams
	if opts.ReadOnly {
		todo.RW = trRO
	} else {
		todo.RW = trRW
	}
	switch level := sql.IsolationLevel(opts.Isolation); level {
	case sql.LevelDefault:
	case sql.LevelReadCommitted:
		todo.Level = trLC
	case sql.LevelSerializable:
		todo.Level = trLS
	default:
		return nil, errors.Errorf("isolation level is not supported: %s", sql.IsolationLevel(opts.Isolation))
	}

	if todo != c.tranParams {
		for _, qry := range []string{todo.RW, todo.Level} {
			if qry == "" {
				continue
			}
			qry = "SET TRANSACTION " + qry
			stmt, err := c.PrepareContext(ctx, qry)
			if err == nil {
				if stc, ok := stmt.(driver.StmtExecContext); ok {
					_, err = stc.ExecContext(ctx, nil)
				} else {
					_, err = stmt.Exec(nil) //lint:ignore SA1019 as that comment is not relevant here
				}
				stmt.Close()
			}
			if err != nil {
				return nil, maybeBadConn(errors.Errorf("%s: %w", qry, err), c)
			}
		}
		c.tranParams = todo
	}

	c.RLock()
	inTran := c.inTransaction
	c.RUnlock()
	if inTran {
		return nil, errors.New("already in transaction")
	}
	c.Lock()
	c.inTransaction = true
	c.Unlock()
	if tt, ok := ctx.Value(traceTagCtxKey).(TraceTag); ok {
		c.Lock()
		c.setTraceTag(tt)
		c.Unlock()
	}
	return c, nil
}

type tranParams struct {
	RW, Level string
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.ensureContextUser(ctx); err != nil {
		return nil, err
	}
	if tt, ok := ctx.Value(traceTagCtxKey).(TraceTag); ok {
		c.Lock()
		c.setTraceTag(tt)
		c.Unlock()
	}
	if query == getConnection {
		if Log != nil {
			Log("msg", "PrepareContext", "shortcut", query)
		}
		return &statement{conn: c, query: query}, nil
	}

	cSQL := C.CString(query)
	defer func() {
		C.free(unsafe.Pointer(cSQL))
	}()
	c.RLock()
	defer c.RUnlock()
	var dpiStmt *C.dpiStmt
	if C.dpiConn_prepareStmt(c.dpiConn, 0, cSQL, C.uint32_t(len(query)), nil, 0,
		(**C.dpiStmt)(unsafe.Pointer(&dpiStmt)),
	) == C.DPI_FAILURE {
		return nil, maybeBadConn(errors.Errorf("Prepare: %s: %w", query, c.getError()), c)
	}
	return &statement{conn: c, dpiStmt: dpiStmt, query: query}, nil
}
func (c *conn) Commit() error {
	return c.iConn.Commit()
}
func (c *conn) Rollback() error {
	return c.iConn.Rollback()
}
func (c *conn) endTran(isCommit bool) error {
	c.Lock()
	c.inTransaction = false
	c.tranParams = tranParams{}

	var err error
	//msg := "Commit"
	if isCommit {
		err = c.iConn.Commit()
	} else {
		err = c.iConn.Rollback()
	}
	c.Unlock()
	return err
}

var _ = driver.Tx((*conn)(nil))

func (c *conn) ServerVersion() (VersionInfo, error) {
	return c.Server, nil
}

func (c *conn) init() error {
	if c.Client.Version == 0 {
		var err error
		if c.Client, err = c.drv.ClientVersion(); err != nil {
			return err
		}
	}
	if c.Server.Version == 0 {
		var err error
		if c.Server, err = c.iConn.ServerVersion(); err != nil {
			return err
		}
	}

	if c.TimeZone != nil && (c.TimeZone != time.Local || c.tzOffSecs != 0) {
		return nil
	}
	c.TimeZone = time.Local
	_, c.tzOffSecs = time.Now().In(c.TimeZone).Zone()
	if Log != nil {
		Log("tz", c.TimeZone, "offSecs", c.tzOffSecs)
	}

	// DBTIMEZONE is useless, false, and misdirecting!
	// https://stackoverflow.com/questions/52531137/sysdate-and-dbtimezone-different-in-oracle-database
	const qry = "SELECT DBTIMEZONE, LTRIM(REGEXP_SUBSTR(TO_CHAR(SYSTIMESTAMP), ' [^ ]+$')) FROM DUAL"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st, err := c.PrepareContext(ctx, qry)
	if err != nil {
		return errors.Errorf("%s: %w", qry, err)
	}
	defer st.Close()
	rows, err := st.Query(nil) //lint:ignore SA1019 - it's hard to use QueryContext here
	if err != nil {
		if Log != nil {
			Log("qry", qry, "error", err)
		}
		return nil
	}
	defer rows.Close()
	var dbTZ, timezone string
	vals := []driver.Value{dbTZ, timezone}
	if err = rows.Next(vals); err != nil && err != io.EOF {
		return errors.Errorf("%s: %w", qry, err)
	}
	dbTZ = vals[0].(string)
	timezone = vals[1].(string)

	tz, off, err := calculateTZ(dbTZ, timezone)
	if Log != nil {
		Log("timezone", timezone, "tz", tz, "offSecs", off)
	}
	if err != nil || tz == nil {
		return err
	}
	c.TimeZone, c.tzOffSecs = tz, off

	return nil
}

func calculateTZ(dbTZ, timezone string) (*time.Location, int, error) {
	if Log != nil {
		Log("dbTZ", dbTZ, "timezone", timezone)
	}
	var tz *time.Location
	now := time.Now()
	_, localOff := time.Now().Local().Zone()
	off := localOff
	var ok bool
	var err error
	if dbTZ != "" && strings.Contains(dbTZ, "/") {
		tz, err = time.LoadLocation(dbTZ)
		if ok = err == nil; ok {
			if tz == time.Local {
				return tz, off, nil
			}
			_, off = now.In(tz).Zone()
		} else if Log != nil {
			Log("LoadLocation", dbTZ, "error", err)
		}
	}
	if !ok {
		if timezone != "" {
			if off, err = parseTZ(timezone); err != nil {
				return tz, off, errors.Errorf("%s: %w", timezone, err)
			}
		} else if off, err = parseTZ(dbTZ); err != nil {
			return tz, off, errors.Errorf("%s: %w", dbTZ, err)
		}
	}
	// This is dangerous, but I just cannot get whether the DB time zone
	// setting has DST or not - DBTIMEZONE returns just a fixed offset.
	if off != localOff && tz == nil {
		tz = time.FixedZone(timezone, off)
	}
	return tz, off, nil
}

func (c *conn) setCallTimeout(ctx context.Context) {
	if c.Client.Version < 18 {
		return
	}
	var dur time.Duration
	if dl, ok := ctx.Deadline(); ok {
		dur - time.Until(dl)
	}
	// force it to be 0 (disabled)
	c.iConn.SetCallTimeout(dur)
}

// maybeBadConn checks whether the error is because of a bad connection, and returns driver.ErrBadConn,
// as database/sql requires.
//
// Also in this case, iff c != nil, closes it.
func maybeBadConn(err error, c *conn) error {
	if err == nil {
		return nil
	}
	cl := func() {}
	if c != nil {
		cl = func() {
			if Log != nil {
				Log("msg", "maybeBadConn close", "conn", c)
			}
			c.close(true)
		}
	}
	if errors.Is(err, driver.ErrBadConn) {
		cl()
		return driver.ErrBadConn
	}
	var cd interface{ Code() int }
	if errors.As(err, &cd) {
		// Yes, this is copied from rana/ora, but I've put it there, so it's mine. @tgulacsi
		switch cd.Code() {
		case 0:
			if strings.Contains(err.Error(), " DPI-1002: ") {
				cl()
				return driver.ErrBadConn
			}
			// cases by experience:
			// ORA-12170: TNS:Connect timeout occurred
			// ORA-12528: TNS:listener: all appropriate instances are blocking new connections
			// ORA-12545: Connect failed because target host or object does not exist
			// ORA-24315: illegal attribute type
			// ORA-28547: connection to server failed, probable Oracle Net admin error
		case 12170, 12528, 12545, 24315, 28547:

			//cases from https://github.com/oracle/odpi/blob/master/src/dpiError.c#L61-L94
		case 22, // invalid session ID; access denied
			28,    // your session has been killed
			31,    // your session has been marked for kill
			45,    // your session has been terminated with no replay
			378,   // buffer pools cannot be created as specified
			602,   // internal programming exception
			603,   // ORACLE server session terminated by fatal error
			609,   // could not attach to incoming connection
			1012,  // not logged on
			1041,  // internal error. hostdef extension doesn't exist
			1043,  // user side memory corruption
			1089,  // immediate shutdown or close in progress
			1092,  // ORACLE instance terminated. Disconnection forced
			2396,  // exceeded maximum idle time, please connect again
			3113,  // end-of-file on communication channel
			3114,  // not connected to ORACLE
			3122,  // attempt to close ORACLE-side window on user side
			3135,  // connection lost contact
			3136,  // inbound connection timed out
			12153, // TNS:not connected
			12537, // TNS:connection closed
			12547, // TNS:lost contact
			12570, // TNS:packet reader failure
			12583, // TNS:no reader
			27146, // post/wait initialization failed
			28511, // lost RPC connection
			56600: // an illegal OCI function call was issued
			cl()
			return driver.ErrBadConn
		}
	}
	return err
}

func (c *conn) setTraceTag(tt TraceTag) error {
	if c == nil || c.iConn == nil {
		return nil
	}
	if c.currentTT.Action == tt.Action &&
		c.currentTT.Module == tt.Module &&
		c.currentTT.ClientInfo == tt.ClientInfo &&
		c.currentTT.ClientIdentifier == tt.ClientIdentifier &&
		c.currentTT.DbOp == tt.DbOp {
		return nil
	}
	if err := c.iConn.SetTraceTag(tt); err != nil {
		return err
	}
	c.currentTT = tt
	return nil
}

const traceTagCtxKey = ctxKey("tracetag")

// ContextWithTraceTag returns a context with the specified TraceTag, which will
// be set on the session used.
func ContextWithTraceTag(ctx context.Context, tt TraceTag) context.Context {
	return context.WithValue(ctx, traceTagCtxKey, tt)
}

// TraceTag holds tracing information for the session. It can be set on the session
// with ContextWithTraceTag.
type TraceTag struct {
	// ClientIdentifier - specifies an end user based on the logon ID, such as HR.HR
	ClientIdentifier string
	// ClientInfo - client-specific info
	ClientInfo string
	// DbOp - database operation
	DbOp string
	// Module - specifies a functional block, such as Accounts Receivable or General Ledger, of an application
	Module string
	// Action - specifies an action, such as an INSERT or UPDATE operation, in a module
	Action string
}

const userpwCtxKey = ctxKey("userPw")

// ContextWithUserPassw returns a context with the specified user and password,
// to be used with heterogeneous pools.
func ContextWithUserPassw(ctx context.Context, user, password string) context.Context {
	return context.WithValue(ctx, userpwCtxKey, [2]string{user, password})
}

func (c *conn) ensureContextUser(ctx context.Context) error {
	if !c.connParams.HeterogeneousPool {
		return nil
	}

	var up [2]string
	var ok bool
	if up, ok = ctx.Value(userpwCtxKey).([2]string); !ok || up[0] == c.currentUser {
		return nil
	}

	if c.dpiConn != nil {
		if err := c.close(false); err != nil {
			return driver.ErrBadConn
		}
	}

	c.Lock()
	defer c.Unlock()

	if err := c.acquireConn(up[0], up[1]); err != nil {
		return err
	}

	return c.init()
}

// StartupMode for the database.
type StartupMode = internal.StartupMode

const (
	// StartupDefault is the default mode for startup which permits database access to all users.
	StartupDefault = internal.StartupDefault
	// StartupForce shuts down a running instance (using ABORT) before starting a new one. This mode should only be used in unusual circumstances.
	StartupForce = internal.StartupForce
	// StartupRestrict only allows database access to users with both the CREATE SESSION and RESTRICTED SESSION privileges (normally the DBA).
	StartupRestrict = internal.StartupRestrict
)

// Startup the database, equivalent to "startup nomount" in SQL*Plus.
// This should be called on PRELIM_AUTH (prelim=1) connection!
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *conn) Startup(mode StartupMode) error {
	return c.iConn.StartupDatabase(mode)
}

// ShutdownMode for the database.
type ShutdownMode = internal.ShutdownMode

const (
	// ShutdownDefault - further connections to the database are prohibited. Wait for users to disconnect from the database.
	ShutdownDefault = internal.ShutdownDefault
	// ShutdownTransactional - further connections to the database are prohibited and no new transactions are allowed to be started. Wait for active transactions to complete.
	ShutdownTransactional = internal.ShutdownTransactional
	// ShutdownTransactionalLocal - behaves the same way as ShutdownTransactional but only waits for local transactions to complete.
	ShutdownTransactionalLocal = internal.ShutdownTransactionalLocal
	// ShutdownImmediate - all uncommitted transactions are terminated and rolled back and all connections to the database are closed immediately.
	ShutdownImmediate = internal.ShutdownImmediate
	// ShutdownAbort - all uncommitted transactions are terminated and are not rolled back. This is the fastest way to shut down the database but the next database startup may require instance recovery.
	ShutdownAbort = internal.ShutdownAbort
	// ShutdownFinal shuts down the database. This mode should only be used in the second call to dpiConn_shutdownDatabase().
	ShutdownFinal = internal.ShutdownFinal
)

// Shutdown shuts down the database.
// Note that this must be done in two phases except in the situation where the instance is aborted.
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *conn) Shutdown(mode ShutdownMode) error {
	return c.iConn.ShutdownDatabase(mode)
}

// Timezone returns the connection's timezone.
func (c *conn) Timezone() *time.Location {
	return c.timeZone
}
