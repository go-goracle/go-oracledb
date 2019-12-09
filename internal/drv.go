// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package internal

/*
#cgo CFLAGS: -I./odpi/include -I./odpi/src -I./odpi/embed

#include <stdlib.h>

#include "dpi.c"
*/
import "C"

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"
)

const (
	// DefaultFetchRowCount is the number of prefetched rows by default (if not changed through FetchRowCount statement option).
	DefaultFetchRowCount = 1 << 8

	// DefaultArraySize is the length of the maximum PL/SQL array by default (if not changed through ArraySize statement option).
	DefaultArraySize = 1 << 10

	// DpiMajorVersion is the wanted major version of the underlying ODPI-C library.
	DpiMajorVersion = C.DPI_MAJOR_VERSION
	// DpiMinorVersion is the wanted minor version of the underlying ODPI-C library.
	DpiMinorVersion = C.DPI_MINOR_VERSION
	// DpiPatchLevel is the patch level version of the underlying ODPI-C library
	DpiPatchLevel = C.DPI_PATCH_LEVEL
	// DpiVersionNumber is the underlying ODPI-C version as one number (Major * 10000 + Minor * 100 + Patch)
	DpiVersionNumber = C.DPI_VERSION_NUMBER

	// DriverName is set on the connection to be seen in the DB
	DriverName = "go-oracledb : " + Version

	// DefaultPoolMinSessions specifies the default value for minSessions for pool creation.
	DefaultPoolMinSessions = 1
	// DefaultPoolMaxSessions specifies the default value for maxSessions for pool creation.
	DefaultPoolMaxSessions = 1000
	// DefaultPoolIncrement specifies the default value for increment for pool creation.
	DefaultPoolIncrement = 1
	// DefaultConnectionClass is the default connectionClass
	DefaultConnectionClass = "GO-ORACLEDB"
	// NoConnectionPoolingConnectionClass is a special connection class name to indicate no connection pooling.
	// It is the same as setting standaloneConnection=1
	NoConnectionPoolingConnectionClass = "NO-CONNECTION-POOLING"
	// DefaultSessionTimeout is the seconds before idle pool sessions get evicted
	DefaultSessionTimeout = 5 * time.Minute
	// DefaultWaitTimeout is the milliseconds to wait for a session to become available
	DefaultWaitTimeout = 30 * time.Second
	// DefaultMaxLifeTime is the maximum time in seconds till a pooled session may exist
	DefaultMaxLifetime = 1 * time.Hour
	// DefaultStatementCacheSize is the default statement cache size
	DefaultStatementCacheSize = 40
)

type (
	CdpiContext C.dpiContext
	CdpiLob     C.dpiLob
	CdpiObject  C.dpiObject
)

func CreateContext() (*CdpiContext, error) {
	var errInfo C.dpiErrorInfo
	var dpiCtx *CdpiContext
	if C.dpiContext_create(C.uint(DpiMajorVersion), C.uint(DpiMinorVersion),
		(**C.dpiContext)(unsafe.Pointer(&dpiCtx)), &errInfo,
	) == C.DPI_FAILURE {
		return nil, fromErrorInfo(errInfo)
	}
	return dpiCtx, nil
}

func (d *CdpiContext) ClientVersion() (VersionInfo, error) {
	var version VersionInfo
	var v C.dpiVersionInfo
	if C.dpiContext_getClientVersion((*C.dpiContext)(d), &v) == C.DPI_FAILURE {
		return version, errors.Errorf("%s: %w", "getClientVersion", d.Err())
	}
	version.set(&v)
	return version, nil
}

type connParams struct {
	Common                       C.dpiCommonCreateParams
	Conn                         C.dpiConnCreateParams
	ExtAuth                      C.int
	Username, Password, Sid      *C.char
	NewPassword, UTF8, ConnClass *C.char
	DriverName                   *C.char
}

func (p *connParams) Close() error {
	F := func(pp **C.char) {
		if pp != nil && *pp != nil {
			C.free(unsafe.Pointer(*pp))
			*pp = nil
		}
	}
	F(&p.Username)
	F(&p.Password)
	F(&p.NewPassword)
	F(&p.Sid)
	F(&p.UTF8)
	F(&p.ConnClass)
	F(&p.DriverName)
	return nil
}

func (d *CdpiContext) prepareConnParams(P ConnectionParams) (*connParams, error) {
	authMode := C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	// OR all the modes together
	for _, elt := range []struct {
		Is   bool
		Mode C.dpiAuthMode
	}{
		{P.IsSysDBA, C.DPI_MODE_AUTH_SYSDBA},
		{P.IsSysOper, C.DPI_MODE_AUTH_SYSOPER},
		{P.IsSysASM, C.DPI_MODE_AUTH_SYSASM},
		{P.IsPrelim, C.DPI_MODE_AUTH_PRELIM},
	} {
		if elt.Is {
			authMode |= elt.Mode
		}
	}
	P.StandaloneConnection = P.StandaloneConnection || P.ConnClass == NoConnectionPoolingConnectionClass
	if P.IsPrelim || P.StandaloneConnection {
		// Prelim: the shared memory may not exist when Oracle is shut down.
		P.ConnClass = ""
	}

	cp := connParams{ExtAuth: C.int(b2i(P.Username == "" && P.Password == ""))}
	if C.dpiContext_initCommonCreateParams((*C.dpiContext)(d), &cp.Common) == C.DPI_FAILURE {
		return nil, errors.Errorf("initCommonCreateParams: %w", d.Err())
	}
	if C.dpiContext_initConnCreateParams((*C.dpiContext)(d), &cp.Conn) == C.DPI_FAILURE {
		return nil, errors.Errorf("initConnCreateParams: %w", d.Err())
	}

	cp.Conn.authMode = authMode
	cp.Conn.externalAuth = cp.ExtAuth
	if P.ConnClass != "" {
		cp.ConnClass = C.CString(P.ConnClass)
		cp.Conn.connectionClass = cp.ConnClass
		cp.Conn.connectionClassLength = C.uint32_t(len(P.ConnClass))
	}

	if !(P.Username == "" && P.Password == "") {
		cp.Username, cp.Password = C.CString(P.Username), C.CString(P.Password)
	}
	if P.SID != "" {
		cp.Sid = C.CString(P.SID)
	}
	cp.UTF8, cp.ConnClass = C.CString("AL32UTF8"), C.CString(P.ConnClass)
	cp.DriverName = C.CString(DriverName)

	cp.Common.createMode = C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED
	if P.EnableEvents {
		cp.Common.createMode |= C.DPI_MODE_CREATE_EVENTS
	}
	cp.Common.encoding = cp.UTF8
	cp.Common.nencoding = cp.UTF8
	cp.Common.driverName = cp.DriverName
	cp.Common.driverNameLength = C.uint32_t(len(DriverName))

	if P.NewPassword != "" {
		cp.NewPassword = C.CString(P.NewPassword)
		cp.Conn.newPassword = cp.NewPassword
		cp.Conn.newPasswordLength = C.uint32_t(len(P.NewPassword))
	}
	return &cp, nil
}

func (d *CdpiContext) OpenConn(P ConnectionParams) (*Conn, error) {
	cp, err := d.prepareConnParams(P)
	if err != nil {
		return nil, err
	}
	defer cp.Close()

	conn := &Conn{dpiContext: d}
	var c C.dpiConn
	conn.dpiConn = &c
	if C.dpiConn_create(
		(*C.dpiContext)(d),
		cp.Username, C.uint32_t(len(P.Username)),
		cp.Password, C.uint32_t(len(P.Password)),
		cp.Sid, C.uint32_t(len(P.SID)),
		&cp.Common,
		&cp.Conn,
		&conn.dpiConn,
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("username=%q sid=%q params=%+v: %w", P.Username, P.SID, cp.Conn, d.Err())
	}
	return conn, nil
}

type Pool struct {
	dpiContext *CdpiContext
	dpiPool    *C.dpiPool
}

func (d *CdpiContext) CreatePool(P ConnectionParams) (*Pool, error) {
	cp, err := d.prepareConnParams(P)
	if err != nil {
		return nil, err
	}
	defer cp.Close()

	var poolCreateParams C.dpiPoolCreateParams
	if C.dpiContext_initPoolCreateParams((*C.dpiContext)(d), &poolCreateParams) == C.DPI_FAILURE {
		return nil, errors.Errorf("initPoolCreateParams: %w", d.Err())
	}
	poolCreateParams.minSessions = DefaultPoolMinSessions
	if P.MinSessions >= 0 {
		poolCreateParams.minSessions = C.uint32_t(P.MinSessions)
	}
	poolCreateParams.maxSessions = DefaultPoolMaxSessions
	if P.MaxSessions > 0 {
		poolCreateParams.maxSessions = C.uint32_t(P.MaxSessions)
	}
	poolCreateParams.sessionIncrement = DefaultPoolIncrement
	if P.PoolIncrement > 0 {
		poolCreateParams.sessionIncrement = C.uint32_t(P.PoolIncrement)
	}
	if cp.ExtAuth == 1 || P.HeterogeneousPool {
		poolCreateParams.homogeneous = 0
	}
	poolCreateParams.externalAuth = cp.ExtAuth
	poolCreateParams.getMode = C.DPI_MODE_POOL_GET_TIMEDWAIT
	poolCreateParams.timeout = C.uint32_t(DefaultSessionTimeout / time.Second)
	if P.SessionTimeout > time.Second {
		poolCreateParams.timeout = C.uint32_t(P.SessionTimeout / time.Second) // seconds before idle pool sessions get evicted
	}
	poolCreateParams.waitTimeout = C.uint32_t(DefaultWaitTimeout / time.Millisecond)
	if P.WaitTimeout > time.Millisecond {
		poolCreateParams.waitTimeout = C.uint32_t(P.WaitTimeout / time.Millisecond) // milliseconds to wait for a session to become available
	}
	poolCreateParams.maxLifetimeSession = C.uint32_t(DefaultMaxLifetime / time.Second)
	if P.MaxLifeTime > 0 {
		poolCreateParams.maxLifetimeSession = C.uint32_t(P.MaxLifeTime / time.Second) // maximum time in seconds till a pooled session may exist
	}

	var commonCreateParams C.dpiCommonCreateParams
	if C.dpiContext_initCommonCreateParams((*C.dpiContext)(d), &commonCreateParams) == C.DPI_FAILURE {
		return nil, errors.Errorf("initCommonCreateParams: %w", d.Err())
	}
	commonCreateParams.createMode = C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED
	if P.EnableEvents {
		commonCreateParams.createMode |= C.DPI_MODE_CREATE_EVENTS
	}
	commonCreateParams.encoding = cp.UTF8
	commonCreateParams.nencoding = cp.UTF8
	commonCreateParams.driverName = cp.DriverName
	commonCreateParams.driverNameLength = C.uint32_t(len(DriverName))

	var p C.dpiPool
	pool := &Pool{dpiContext: d, dpiPool: &p}
	if C.dpiPool_create(
		(*C.dpiContext)(d),
		cp.Username, C.uint32_t(len(P.Username)),
		cp.Password, C.uint32_t(len(P.Password)),
		cp.Sid, C.uint32_t(len(P.SID)),
		&cp.Common,
		&poolCreateParams,
		&pool.dpiPool,
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("params=%s extAuth=%v: %w", P.String(), cp.ExtAuth, d.Err())
	}
	if P.StatementCacheSize > 0 {
		C.dpiPool_setStmtCacheSize(pool.dpiPool, C.uint(P.StatementCacheSize))
	}

	return pool, nil
}

func (dp *Pool) AcquireConn(d CdpiContext, user, pass string) (*Conn, error) {
	var connCreateParams C.dpiConnCreateParams
	if C.dpiContext_initConnCreateParams((*C.dpiContext)(dp.dpiContext), &connCreateParams) == C.DPI_FAILURE {
		return nil, errors.Errorf("initConnCreateParams: %w", "", d.Err())
	}

	var cUserName, cPassword *C.char
	defer func() {
		if cUserName != nil {
			C.free(unsafe.Pointer(cUserName))
		}
		if cPassword != nil {
			C.free(unsafe.Pointer(cPassword))
		}
	}()
	if user != "" {
		cUserName = C.CString(user)
	}
	if pass != "" {
		cPassword = C.CString(pass)
	}

	var c C.dpiConn
	conn := &Conn{dpiContext: dp.dpiContext, dpiConn: &c}
	if C.dpiPool_acquireConnection(
		dp.dpiPool,
		cUserName, C.uint32_t(len(user)), cPassword, C.uint32_t(len(pass)),
		&connCreateParams,
		(**C.dpiConn)(unsafe.Pointer(&conn.dpiConn)),
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("acquirePoolConnection: %w", d.Err())
	}

	return conn, nil
}

// ConnectionParams holds the params for a connection (pool).
// You can use ConnectionParams{...}.StringWithPassword()
// as a connection string in sql.Open.
type ConnectionParams struct {
	Username, Password, SID, ConnClass string
	// NewPassword is used iff StandaloneConnection is true!
	NewPassword                              string
	MinSessions, MaxSessions, PoolIncrement  int
	StatementCacheSize                       int
	WaitTimeout, MaxLifeTime, SessionTimeout time.Duration
	IsSysDBA, IsSysOper, IsSysASM, IsPrelim  bool
	HeterogeneousPool                        bool
	StandaloneConnection                     bool
	EnableEvents                             bool
	Timezone                                 *time.Location
}

// String returns the string representation of ConnectionParams.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) String() string {
	return P.string(true, false)
}

// StringNoClass returns the string representation of ConnectionParams, without class info.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) StringNoClass() string {
	return P.string(false, false)
}

// StringWithPassword returns the string representation of ConnectionParams (as String() does),
// but does NOT obfuscate the password, just prints it as is.
func (P ConnectionParams) StringWithPassword() string {
	return P.string(true, true)
}

func (P ConnectionParams) string(class, withPassword bool) string {
	host, path := P.SID, ""
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host, path = host[:i], host[i:]
	}
	q := make(url.Values, 32)
	s := P.ConnClass
	if !class {
		s = ""
	}
	q.Add("connectionClass", s)

	password := P.Password
	if withPassword {
		q.Add("newPassword", P.NewPassword)
	} else {
		hsh := fnv.New64()
		io.WriteString(hsh, P.Password)
		password = "SECRET-" + base64.URLEncoding.EncodeToString(hsh.Sum(nil))
		if P.NewPassword != "" {
			hsh.Reset()
			io.WriteString(hsh, P.NewPassword)
			q.Add("newPassword", "SECRET-"+base64.URLEncoding.EncodeToString(hsh.Sum(nil)))
		}
	}
	s = ""
	if P.Timezone != nil {
		s = P.Timezone.String()
	}
	q.Add("timezone", s)
	B := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	q.Add("poolMinSessions", strconv.Itoa(P.MinSessions))
	q.Add("poolMaxSessions", strconv.Itoa(P.MaxSessions))
	q.Add("poolIncrement", strconv.Itoa(P.PoolIncrement))
	q.Add("sysdba", B(P.IsSysDBA))
	q.Add("sysoper", B(P.IsSysOper))
	q.Add("sysasm", B(P.IsSysASM))
	q.Add("standaloneConnection", B(P.StandaloneConnection))
	q.Add("enableEvents", B(P.EnableEvents))
	q.Add("heterogeneousPool", B(P.HeterogeneousPool))
	q.Add("prelim", B(P.IsPrelim))
	q.Add("poolWaitTimeout", P.WaitTimeout.String())
	q.Add("poolSessionMaxLifetime", P.MaxLifeTime.String())
	q.Add("poolSessionTimeout", P.SessionTimeout.String())
	return (&url.URL{
		Scheme:   "oracle",
		User:     url.UserPassword(P.Username, password),
		Host:     host,
		Path:     path,
		RawQuery: q.Encode(),
	}).String()
}

// ParseConnString parses the given connection string into a struct.
func ParseConnString(connString string) (ConnectionParams, error) {
	P := ConnectionParams{
		MinSessions:    DefaultPoolMinSessions,
		MaxSessions:    DefaultPoolMaxSessions,
		PoolIncrement:  DefaultPoolIncrement,
		ConnClass:      DefaultConnectionClass,
		MaxLifeTime:    DefaultMaxLifetime,
		WaitTimeout:    DefaultWaitTimeout,
		SessionTimeout: DefaultSessionTimeout,
	}
	if !strings.HasPrefix(connString, "oracle://") {
		i := strings.IndexByte(connString, '/')
		if i < 0 {
			return P, errors.New("no '/' in connection string")
		}
		P.Username, connString = connString[:i], connString[i+1:]

		uSid := strings.ToUpper(connString)
		//fmt.Printf("connString=%q SID=%q\n", connString, uSid)
		if strings.Contains(uSid, " AS ") {
			if P.IsSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); P.IsSysDBA {
				connString = connString[:len(connString)-10]
			} else if P.IsSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); P.IsSysOper {
				connString = connString[:len(connString)-11]
			} else if P.IsSysASM = strings.HasSuffix(uSid, " AS SYSASM"); P.IsSysASM {
				connString = connString[:len(connString)-10]
			}
		}
		if i = strings.IndexByte(connString, '@'); i >= 0 {
			P.Password, P.SID = connString[:i], connString[i+1:]
		} else {
			P.Password = connString
		}
		if strings.HasSuffix(P.SID, ":POOLED") {
			P.ConnClass, P.SID = "POOLED", P.SID[:len(P.SID)-7]
		}
		//fmt.Printf("connString=%q params=%s\n", connString, P)
		return P, nil
	}
	u, err := url.Parse(connString)
	if err != nil {
		return P, errors.Errorf("%s: %w", connString, err)
	}
	if usr := u.User; usr != nil {
		P.Username = usr.Username()
		P.Password, _ = usr.Password()
	}
	P.SID = u.Hostname()
	// IPv6 literal address brackets are removed by u.Hostname,
	// so we have to put them back
	if strings.HasPrefix(u.Host, "[") && !strings.Contains(P.SID[1:], "]") {
		P.SID = "[" + P.SID + "]"
	}
	if u.Port() != "" {
		P.SID += ":" + u.Port()
	}
	if u.Path != "" && u.Path != "/" {
		P.SID += u.Path
	}
	q := u.Query()
	if vv, ok := q["connectionClass"]; ok {
		P.ConnClass = vv[0]
	}
	for _, task := range []struct {
		Dest *bool
		Key  string
	}{
		{&P.IsSysDBA, "sysdba"},
		{&P.IsSysOper, "sysoper"},
		{&P.IsSysASM, "sysasm"},
		{&P.IsPrelim, "prelim"},

		{&P.StandaloneConnection, "standaloneConnection"},
		{&P.EnableEvents, "enableEvents"},
		{&P.HeterogeneousPool, "heterogeneousPool"},
	} {
		*task.Dest = q.Get(task.Key) == "1"
	}
	if tz := q.Get("timezone"); tz != "" {
		if tz == "local" {
			P.Timezone = time.Local
		} else if strings.Contains(tz, "/") {
			if P.Timezone, err = time.LoadLocation(tz); err != nil {
				return P, errors.Errorf("%s: %w", tz, err)
			}
		} else if off, err := parseTZ(tz); err == nil {
			P.Timezone = time.FixedZone(tz, off)
		} else {
			return P, errors.Errorf("%s: %w", tz, err)
		}
	}

	P.StandaloneConnection = P.StandaloneConnection || P.ConnClass == NoConnectionPoolingConnectionClass
	if P.IsPrelim {
		P.ConnClass = ""
	}
	if P.StandaloneConnection {
		P.NewPassword = q.Get("newPassword")
	}

	for _, task := range []struct {
		Dest *int
		Key  string
	}{
		{&P.MinSessions, "poolMinSessions"},
		{&P.MaxSessions, "poolMaxSessions"},
		{&P.PoolIncrement, "poolIncrement"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = strconv.Atoi(s)
		if err != nil {
			return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
		}
	}
	for _, task := range []struct {
		Dest *time.Duration
		Key  string
	}{
		{&P.SessionTimeout, "poolSessionTimeout"},
		{&P.WaitTimeout, "poolWaitTimeout"},
		{&P.MaxLifeTime, "poolSessionMaxLifetime"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = time.ParseDuration(s)
		if err != nil {
			if !strings.Contains(err.Error(), "time: missing unit in duration") {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			i, err := strconv.Atoi(s)
			if err != nil {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			base := time.Second
			if task.Key == "poolWaitTimeout" {
				base = time.Millisecond
			}
			*task.Dest = time.Duration(i) * base
		}
	}
	if P.MinSessions > P.MaxSessions {
		P.MinSessions = P.MaxSessions
	}
	if P.MinSessions == P.MaxSessions {
		P.PoolIncrement = 0
	} else if P.PoolIncrement < 1 {
		P.PoolIncrement = 1
	}
	return P, nil
}

// OraErr is an error holding the ORA-01234 code and the message.
type OraErr struct {
	message string
	code    int
}

// AsOraErr returns the underlying *OraErr and whether it succeeded.
func AsOraErr(err error) (*OraErr, bool) {
	var oerr *OraErr
	ok := errors.As(err, &oerr)
	return oerr, ok
}

var _ = error((*OraErr)(nil))

// Code returns the OraErr's error code.
func (oe *OraErr) Code() int { return oe.code }

// Message returns the OraErr's message.
func (oe *OraErr) Message() string { return oe.message }
func (oe *OraErr) Error() string {
	msg := oe.Message()
	if oe.code == 0 && msg == "" {
		return ""
	}
	return fmt.Sprintf("ORA-%05d: %s", oe.code, oe.message)
}
func fromErrorInfo(errInfo C.dpiErrorInfo) *OraErr {
	oe := OraErr{
		code:    int(errInfo.code),
		message: strings.TrimSpace(C.GoString(errInfo.message)),
	}
	if oe.code == 0 && strings.HasPrefix(oe.message, "ORA-") &&
		len(oe.message) > 9 && oe.message[9] == ':' {
		if i, _ := strconv.Atoi(oe.message[4:9]); i > 0 {
			oe.code = i
		}
	}
	oe.message = strings.TrimPrefix(oe.message, fmt.Sprintf("ORA-%05d: ", oe.Code()))
	return &oe
}

// newErrorInfo is just for testing: testing cannot use Cgo...
func newErrorInfo(code int, message string) C.dpiErrorInfo {
	return C.dpiErrorInfo{code: C.int32_t(code), message: C.CString(message)}
}

// against deadcode
var _ = newErrorInfo

func (d *CdpiContext) Err() *OraErr {
	if d == nil {
		return &OraErr{code: -12153, message: driver.ErrBadConn.Error()}
	}
	var errInfo C.dpiErrorInfo
	C.dpiContext_getError((*C.dpiContext)(d), &errInfo)
	return fromErrorInfo(errInfo)
}

// VersionInfo holds version info returned by Oracle DB.
type VersionInfo struct {
	ServerRelease                                           string
	Version, Release, Update, PortRelease, PortUpdate, Full int
}

func (V *VersionInfo) set(v *C.dpiVersionInfo) {
	*V = VersionInfo{
		Version: int(v.versionNum),
		Release: int(v.releaseNum), Update: int(v.updateNum),
		PortRelease: int(v.portReleaseNum), PortUpdate: int(v.portUpdateNum),
		Full: int(v.fullVersionNum),
	}
}
func (V VersionInfo) String() string {
	var s string
	if V.ServerRelease != "" {
		s = " [" + V.ServerRelease + "]"
	}
	return fmt.Sprintf("%d.%d.%d.%d.%d%s", V.Version, V.Release, V.Update, V.PortRelease, V.PortUpdate, s)
}

var timezones = make(map[[2]C.int8_t]*time.Location)
var timezonesMu sync.RWMutex

func timeZoneFor(hourOffset, minuteOffset C.int8_t) *time.Location {
	if hourOffset == 0 && minuteOffset == 0 {
		return time.UTC
	}
	key := [2]C.int8_t{hourOffset, minuteOffset}
	timezonesMu.RLock()
	tz := timezones[key]
	timezonesMu.RUnlock()
	if tz == nil {
		timezonesMu.Lock()
		if tz = timezones[key]; tz == nil {
			tz = time.FixedZone(
				fmt.Sprintf("%02d:%02d", hourOffset, minuteOffset),
				int(hourOffset)*3600+int(minuteOffset)*60,
			)
			timezones[key] = tz
		}
		timezonesMu.Unlock()
	}
	return tz
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
func parseTZ(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, io.EOF
	}
	if s == "Z" || s == "UTC" {
		return 0, nil
	}
	var tz int
	var ok bool
	if i := strings.IndexByte(s, ':'); i >= 0 {
		if i64, err := strconv.ParseInt(s[i+1:], 10, 6); err != nil {
			return tz, errors.Errorf("%s: %w", s, err)
		} else {
			tz = int(i64 * 60)
		}
		s = s[:i]
		ok = true
	}
	if !ok {
		if i := strings.IndexByte(s, '/'); i >= 0 {
			targetLoc, err := time.LoadLocation(s)
			if err != nil {
				return tz, errors.Errorf("%s: %w", s, err)
			}

			_, localOffset := time.Now().In(targetLoc).Zone()

			tz = localOffset
			return tz, nil
		}
	}
	if i64, err := strconv.ParseInt(s, 10, 5); err != nil {
		return tz, errors.Errorf("%s: %w", s, err)
	} else {
		if i64 < 0 {
			tz = -tz
		}
		tz += int(i64 * 3600)
	}
	return tz, nil
}
