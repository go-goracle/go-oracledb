// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package internal

/*
#include <stdlib.h>
#include "dpiImpl.h"
*/
import "C"

import (
	"bytes"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"
)

// The maximum capacity is limited to (2^32 / sizeof(dpiData))-1 to remain compatible
// with 32-bit platforms. The size of a `C.dpiData` is 32 Byte on a 64-bit system, `C.dpiSubscrMessageTable` is 40 bytes.
// So this is 2^25.
// See https://github.com/go-goracle/goracle/issues/73#issuecomment-401281714
const maxArraySize = (1<<32)/C.sizeof_dpiSubscrMessageTable - 1

type Conn struct {
	Ctx  *CdpiContext
	Conn *C.dpiConn
}

func (c *Conn) Err() error {
	if c == nil || c.Ctx == nil {
		return nil
	}
	return c.Ctx.Err()
}

func (c *Conn) Break() error {
	if C.dpiConn_breakExecution(c.Conn) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

// Ping checks the connection's state.
func (c *Conn) Ping() error {
	if C.dpiConn_ping(c.Conn) == C.DPI_FAILURE {
		return errors.Errorf("Ping: %w", c.Err())
	}
	return nil
}

func (c *Conn) Release() error {
	if c == nil {
		return nil
	}
	dpiConn := c.Conn
	c.Conn = nil
	if dpiConn == nil {
		return nil
	}
	if C.dpiConn_release(dpiConn) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *Conn) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	defer c.Release()
	if C.dpiConn_close(c.Conn, C.DPI_MODE_CONN_CLOSE_DROP, nil, 0) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

type CdpiStmt C.dpiStmt

// PrepareStmt returns a prepared statement, bound to this connection.
func (c *Conn) PrepareStmt(query string) (*CdpiStmt, error) {
	cSQL := C.CString(query)
	defer C.free(unsafe.Pointer(cSQL))
	var stmt CdpiStmt
	dpiStmt := &stmt
	if C.dpiConn_prepareStmt(c.Conn, 0, cSQL, C.uint32_t(len(query)), nil, 0,
		(**C.dpiStmt)(unsafe.Pointer(&dpiStmt)),
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("Prepare: %s: %w", query, c.Err())
	}
	return dpiStmt, nil
}
func (c *Conn) Commit() error {
	if C.dpiConn_commit(c.Conn) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

func (c *Conn) Rollback() error {
	if C.dpiConn_rollback(c.Conn) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

type (
	CdpiObjectType    C.dpiObjectType
	CdpiNativeTypeNum C.dpiNativeTypeNum
	CdpiOracleTypeNum C.dpiOracleTypeNum

	CdpiVar  C.dpiVar
	CdpiData C.dpiData
)

type varInfo struct {
	SliceLen, BufSize int
	ObjectType        *CdpiObjectType
	NatTyp            CdpiNativeTypeNum
	Typ               CdpiOracleTypeNum
	IsPLSArray        bool
}

func (c *Conn) NewVar(vi varInfo) (*CdpiVar, []CdpiData, error) {
	if c == nil || c.Conn == nil {
		return nil, nil, errors.New("connection is nil")
	}
	isArray := C.int(0)
	if vi.IsPLSArray {
		isArray = 1
	}
	if vi.SliceLen < 1 {
		vi.SliceLen = 1
	}
	var dataArr *CdpiData
	var v *CdpiVar
	if C.dpiConn_newVar(
		c.Conn, (C.dpiOracleTypeNum)(vi.Typ), (C.dpiNativeTypeNum)(vi.NatTyp), C.uint32_t(vi.SliceLen),
		C.uint32_t(vi.BufSize), 1,
		isArray, (*C.dpiObjectType)(vi.ObjectType),
		(**C.dpiVar)(unsafe.Pointer(&v)), (**C.dpiData)(unsafe.Pointer(&dataArr)),
	) == C.DPI_FAILURE {
		return nil, nil, errors.Errorf("newVar(typ=%d, natTyp=%d, sliceLen=%d, bufSize=%d): %w", vi.Typ, vi.NatTyp, vi.SliceLen, vi.BufSize, c.Err())
	}
	// https://github.com/golang/go/wiki/cgo#Turning_C_arrays_into_Go_slices
	/*
		var theCArray *C.YourType = C.getTheArray()
		length := C.getTheArrayLength()
		slice := (*[maxArraySize]C.YourType)(unsafe.Pointer(theCArray))[:length:length]
	*/
	data := ((*[maxArraySize]CdpiData)(unsafe.Pointer(dataArr)))[:vi.SliceLen:vi.SliceLen]
	return v, data, nil
}

func (c *Conn) ServerVersion() (VersionInfo, error) {
	var version VersionInfo
	var v C.dpiVersionInfo
	var release *C.char
	var releaseLen C.uint32_t
	if C.dpiConn_getServerVersion(c.Conn, &release, &releaseLen, &v) == C.DPI_FAILURE {
		return version, c.Err()
	}
	version.set(&v)
	version.ServerRelease = string(bytes.ReplaceAll(
		((*[maxArraySize]byte)(unsafe.Pointer(release)))[:releaseLen:releaseLen],
		[]byte{'\n'}, []byte{';', ' '}))
	return version, nil
}

func (c *Conn) SetCallTimeout(dur time.Duration) error {
	ms := C.uint32_t(dur / time.Millisecond)
	if C.dpiConn_setCallTimeout(c.Conn, ms) == C.DPI_FAILURE {
		return c.Err()
	}
	return nil
}

func (c *Conn) SetTraceTag(tt TraceTag) error {
	if c == nil || c.Conn == nil {
		return nil
	}
	for nm, v := range map[string]string{
		"action":     tt.Action,
		"module":     tt.Module,
		"info":       tt.ClientInfo,
		"identifier": tt.ClientIdentifier,
		"op":         tt.DbOp,
	} {
		var s *C.char
		if v != "" {
			s = C.CString(v)
		}
		var rc C.int
		switch nm {
		case "action":
			rc = C.dpiConn_setAction(c.Conn, s, C.uint32_t(len(v)))
		case "module":
			rc = C.dpiConn_setModule(c.Conn, s, C.uint32_t(len(v)))
		case "info":
			rc = C.dpiConn_setClientInfo(c.Conn, s, C.uint32_t(len(v)))
		case "identifier":
			rc = C.dpiConn_setClientIdentifier(c.Conn, s, C.uint32_t(len(v)))
		case "op":
			rc = C.dpiConn_setDbOp(c.Conn, s, C.uint32_t(len(v)))
		}
		if s != nil {
			C.free(unsafe.Pointer(s))
		}
		if rc == C.DPI_FAILURE {
			return errors.Errorf("%s: %w", nm, c.Err())
		}
	}
	return nil
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

// StartupMode for the database.
type StartupMode C.dpiStartupMode

const (
	// StartupDefault is the default mode for startup which permits database access to all users.
	StartupDefault = StartupMode(C.DPI_MODE_STARTUP_DEFAULT)
	// StartupForce shuts down a running instance (using ABORT) before starting a new one. This mode should only be used in unusual circumstances.
	StartupForce = StartupMode(C.DPI_MODE_STARTUP_FORCE)
	// StartupRestrict only allows database access to users with both the CREATE SESSION and RESTRICTED SESSION privileges (normally the DBA).
	StartupRestrict = StartupMode(C.DPI_MODE_STARTUP_RESTRICT)
)

// Startup the database, equivalent to "startup nomount" in SQL*Plus.
// This should be called on PRELIM_AUTH (prelim=1) connection!
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *Conn) Startup(mode StartupMode) error {
	if C.dpiConn_startupDatabase(c.Conn, C.dpiStartupMode(mode)) == C.DPI_FAILURE {
		return errors.Errorf("startup(%v): %w", mode, c.Err())
	}
	return nil
}

// ShutdownMode for the database.
type ShutdownMode C.dpiShutdownMode

const (
	// ShutdownDefault - further connections to the database are prohibited. Wait for users to disconnect from the database.
	ShutdownDefault = ShutdownMode(C.DPI_MODE_SHUTDOWN_DEFAULT)
	// ShutdownTransactional - further connections to the database are prohibited and no new transactions are allowed to be started. Wait for active transactions to complete.
	ShutdownTransactional = ShutdownMode(C.DPI_MODE_SHUTDOWN_TRANSACTIONAL)
	// ShutdownTransactionalLocal - behaves the same way as ShutdownTransactional but only waits for local transactions to complete.
	ShutdownTransactionalLocal = ShutdownMode(C.DPI_MODE_SHUTDOWN_TRANSACTIONAL_LOCAL)
	// ShutdownImmediate - all uncommitted transactions are terminated and rolled back and all connections to the database are closed immediately.
	ShutdownImmediate = ShutdownMode(C.DPI_MODE_SHUTDOWN_IMMEDIATE)
	// ShutdownAbort - all uncommitted transactions are terminated and are not rolled back. This is the fastest way to shut down the database but the next database startup may require instance recovery.
	ShutdownAbort = ShutdownMode(C.DPI_MODE_SHUTDOWN_ABORT)
	// ShutdownFinal shuts down the database. This mode should only be used in the second call to dpiConn_shutdownDatabase().
	ShutdownFinal = ShutdownMode(C.DPI_MODE_SHUTDOWN_FINAL)
)

// Shutdown shuts down the database.
// Note that this must be done in two phases except in the situation where the instance is aborted.
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *Conn) Shutdown(mode ShutdownMode) error {
	if C.dpiConn_shutdownDatabase(c.Conn, C.dpiShutdownMode(mode)) == C.DPI_FAILURE {
		return errors.Errorf("shutdown(%v): %w", mode, c.Err())
	}
	return nil
}
