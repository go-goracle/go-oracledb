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
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"
)

var ErrNotSupported = errors.New("not supported")

// Data holds the data to/from Oracle.
type Data struct {
	dpiData       C.dpiData
	ObjectType    ObjectType
	NativeTypeNum CdpiNativeTypeNum
}

// NewData creates a new Data structure for the given type, populated with the given type.
func NewData() *Data {
	d := &Data{}
	d.dpiData.isNull = 1
	return d
}

// IsNull returns whether the data is null.
func (d *Data) IsNull() bool {
	// Use of C.dpiData_getIsNull(d) would be safer,
	// but ODPI-C 3.1.4 just returns dpiData->isNull, so do the same
	// without calling CGO.
	return d == nil || d.dpiData.isNull == 1
}

// SetNull sets the value of the data to be the null value.
func (d *Data) SetNull() {
	if !d.IsNull() {
		// Maybe C.dpiData_setNull(d) would be safer, but as we don't use C.dpiData_getIsNull,
		// and those functions (at least in ODPI-C 3.1.4) just operate on data->isNull directly,
		// don't use CGO if possible.
		d.dpiData.isNull = 1
	}
}

// GetBool returns the bool data.
func (d *Data) GetBool() bool {
	return !d.IsNull() && C.dpiData_getBool(&d.dpiData) == 1
}

// SetBool sets the data as bool.
func (d *Data) SetBool(b bool) {
	var i C.int
	if b {
		i = 1
	}
	C.dpiData_setBool(&d.dpiData, i)
}

// GetBytes returns the []byte from the data.
func (d *Data) GetBytes() []byte {
	if d.IsNull() {
		return nil
	}
	b := C.dpiData_getBytes(&d.dpiData)
	if b.ptr == nil || b.length == 0 {
		return nil
	}
	return ((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length]
}

// SetBytes set the data as []byte.
func (d *Data) SetBytes(b []byte) {
	if b == nil {
		d.dpiData.isNull = 1
		return
	}
	C.dpiData_setBytes(&d.dpiData, (*C.char)(unsafe.Pointer(&b[0])), C.uint32_t(len(b)))
}

// GetFloat32 gets float32 from the data.
func (d *Data) GetFloat32() float32 {
	if d.IsNull() {
		return 0
	}
	return float32(C.dpiData_getFloat(&d.dpiData))
}

// SetFloat32 sets the data as float32.
func (d *Data) SetFloat32(f float32) {
	C.dpiData_setFloat(&d.dpiData, C.float(f))
}

// GetFloat64 gets float64 from the data.
func (d *Data) GetFloat64() float64 {
	//fmt.Println("GetFloat64", d.IsNull(), d)
	if d.IsNull() {
		return 0
	}
	return float64(C.dpiData_getDouble(&d.dpiData))
}

// SetFloat64 sets the data as float64.
func (d *Data) SetFloat64(f float64) {
	C.dpiData_setDouble(&d.dpiData, C.double(f))
}

// GetInt64 gets int64 from the data.
func (d *Data) GetInt64() int64 {
	if d.IsNull() {
		return 0
	}
	return int64(C.dpiData_getInt64(&d.dpiData))
}

// SetInt64 sets the data as int64.
func (d *Data) SetInt64(i int64) {
	C.dpiData_setInt64(&d.dpiData, C.int64_t(i))
}

// GetIntervalDS gets duration as interval date-seconds from data.
func (d *Data) GetIntervalDS() time.Duration {
	if d.IsNull() {
		return 0
	}
	ds := C.dpiData_getIntervalDS(&d.dpiData)
	return time.Duration(ds.days)*24*time.Hour +
		time.Duration(ds.hours)*time.Hour +
		time.Duration(ds.minutes)*time.Minute +
		time.Duration(ds.seconds)*time.Second +
		time.Duration(ds.fseconds)
}

// SetIntervalDS sets the duration as interval date-seconds to data.
func (d *Data) SetIntervalDS(dur time.Duration) {
	C.dpiData_setIntervalDS(&d.dpiData,
		C.int32_t(int64(dur.Hours())/24),
		C.int32_t(int64(dur.Hours())%24), C.int32_t(dur.Minutes()), C.int32_t(dur.Seconds()),
		C.int32_t(dur.Nanoseconds()),
	)
}

// GetIntervalYM gets IntervalYM from the data.
func (d *Data) GetIntervalYM() IntervalYM {
	if d.IsNull() {
		return IntervalYM{}
	}
	ym := C.dpiData_getIntervalYM(&d.dpiData)
	return IntervalYM{Years: int(ym.years), Months: int(ym.months)}
}

// SetIntervalYM sets IntervalYM to the data.
func (d *Data) SetIntervalYM(ym IntervalYM) {
	C.dpiData_setIntervalYM(&d.dpiData, C.int32_t(ym.Years), C.int32_t(ym.Months))
}

// GetLob gets data as Lob.
func (d *Data) GetLob() *CdpiLob {
	if d.IsNull() {
		return nil
	}
	return (*CdpiLob)(C.dpiData_getLOB(&d.dpiData))
}

// SetLob sets Lob to the data.
func (d *Data) SetLob(lob *Lob) {
	C.dpiData_setLOB(&d.dpiData, (*C.dpiLob)(lob.dpiLob))
}

// GetObject gets Object from data.
//
// As with all Objects, you MUST call Close on it when not needed anymore!
func (d *Data) GetObject() *Object {
	if d == nil {
		panic("null")
	}
	if d.IsNull() {
		return nil
	}

	o := C.dpiData_getObject(&d.dpiData)
	if o == nil {
		return nil
	}
	if C.dpiObject_addRef(o) == C.DPI_FAILURE {
		panic(d.ObjectType.Err())
	}
	obj := &Object{dpiObject: o, ObjectType: d.ObjectType}
	obj.init()
	return obj
}

// SetObject sets Object to data.
func (d *Data) SetObject(o *Object) {
	C.dpiData_setObject(&d.dpiData, o.dpiObject)
}

// GetStmt gets Stmt from data.
func (d *Data) GetStmt() *CdpiStmt {
	if d.IsNull() {
		return nil
	}
	return (*CdpiStmt)(C.dpiData_getStmt(&d.dpiData))
}

// SetStmt sets Stmt to data.
func (d *Data) SetStmt(s *CdpiStmt) {
	C.dpiData_setStmt(&d.dpiData, (*C.dpiStmt)(s))
}

// GetTime gets Time from data.
func (d *Data) GetTime() time.Time {
	if d.IsNull() {
		return time.Time{}
	}
	ts := C.dpiData_getTimestamp(&d.dpiData)
	return time.Date(
		int(ts.year), time.Month(ts.month), int(ts.day),
		int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
		timeZoneFor(ts.tzHourOffset, ts.tzMinuteOffset),
	)

}

// SetTime sets Time to data.
func (d *Data) SetTime(t time.Time) {
	_, z := t.Zone()
	C.dpiData_setTimestamp(&d.dpiData,
		C.int16_t(t.Year()), C.uint8_t(t.Month()), C.uint8_t(t.Day()),
		C.uint8_t(t.Hour()), C.uint8_t(t.Minute()), C.uint8_t(t.Second()), C.uint32_t(t.Nanosecond()),
		C.int8_t(z/3600), C.int8_t((z%3600)/60),
	)
}

// GetUint64 gets data as uint64.
func (d *Data) GetUint64() uint64 {
	if d.IsNull() {
		return 0
	}
	return uint64(C.dpiData_getUint64(&d.dpiData))
}

// SetUint64 sets data to uint64.
func (d *Data) SetUint64(u uint64) {
	C.dpiData_setUint64(&d.dpiData, C.uint64_t(u))
}

// IntervalYM holds Years and Months as interval.
type IntervalYM struct {
	Years, Months int
}

// Get returns the contents of Data.
func (d *Data) Get() interface{} {
	switch d.NativeTypeNum {
	case C.DPI_NATIVE_TYPE_BOOLEAN:
		return d.GetBool()
	case C.DPI_NATIVE_TYPE_BYTES:
		return d.GetBytes()
	case C.DPI_NATIVE_TYPE_DOUBLE:
		return d.GetFloat64()
	case C.DPI_NATIVE_TYPE_FLOAT:
		return d.GetFloat32()
	case C.DPI_NATIVE_TYPE_INT64:
		return d.GetInt64()
	case C.DPI_NATIVE_TYPE_INTERVAL_DS:
		return d.GetIntervalDS()
	case C.DPI_NATIVE_TYPE_INTERVAL_YM:
		return d.GetIntervalYM()
	case C.DPI_NATIVE_TYPE_LOB:
		return d.GetLob()
	case C.DPI_NATIVE_TYPE_OBJECT:
		return d.GetObject()
	case C.DPI_NATIVE_TYPE_STMT:
		return d.GetStmt()
	case C.DPI_NATIVE_TYPE_TIMESTAMP:
		return d.GetTime()
	case C.DPI_NATIVE_TYPE_UINT64:
		return d.GetUint64()
	default:
		panic(fmt.Sprintf("unknown NativeTypeNum=%d", d.NativeTypeNum))
	}
}

// Set the data.
func (d *Data) Set(v interface{}) error {
	if v == nil {
		return errors.Errorf("%s: %w", "nil type", ErrNotSupported)
	}
	d.dpiData.isNull = 1
	switch x := v.(type) {
	case int32:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_INT64
		d.SetInt64(int64(x))
	case int64:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_INT64
		d.SetInt64(x)
	case uint64:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_UINT64
		d.SetUint64(x)
	case float32:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_FLOAT
		d.SetFloat32(x)
	case float64:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_DOUBLE
		d.SetFloat64(x)
	case string:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
		d.SetBytes([]byte(x))
	case []byte:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
		d.SetBytes(x)
	case time.Time:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_TIMESTAMP
		d.SetTime(x)
	case time.Duration:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_INTERVAL_DS
		d.SetIntervalDS(x)
	case IntervalYM:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_INTERVAL_YM
		d.SetIntervalYM(x)
	case *Lob:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_LOB
		d.SetLob(x)
	case *Object:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_OBJECT
		d.ObjectType = x.ObjectType
		d.SetObject(x)
	//case *stmt:
	//d.NativeTypeNum = C.DPI_NATIVE_TYPE_STMT
	//d.SetStmt(x)
	case bool:
		d.NativeTypeNum = C.DPI_NATIVE_TYPE_BOOLEAN
		d.SetBool(x)
	//case rowid:
	//d.NativeTypeNum = C.DPI_NATIVE_TYPE_ROWID
	//d.SetRowid(x)
	default:
		return errors.Errorf("%T: %w", ErrNotSupported, v)
	}
	return nil
}

// IsObject returns whether the data contains an Object or not.
func (d *Data) IsObject() bool {
	return d.NativeTypeNum == C.DPI_NATIVE_TYPE_OBJECT
}

// NewData returns Data for input parameters on Object/ObjectCollection.
func (c *Conn) NewData(baseType interface{}, sliceLen, bufSize int) ([]Data, error) {
	if c == nil || c.dpiConn == nil {
		return nil, errors.New("connection is nil")
	}

	vi, err := NewVarInfo(baseType, sliceLen, bufSize)
	if err != nil {
		return nil, err
	}

	data := make([]Data, sliceLen)
	for i := 0; i < sliceLen; i++ {
		data[i].NativeTypeNum = vi.NatTyp
	}

	return data, nil
}

type VarInfo struct {
	SliceLen, BufSize int
	ObjectType        *CdpiObjectType
	NatTyp            CdpiNativeTypeNum
	Typ               CdpiOracleTypeNum
	IsPLSArray        bool
}

func (c *Conn) NewVar(vi VarInfo) (*CdpiVar, []CdpiData, error) {
	if c == nil || c.dpiConn == nil {
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
		c.dpiConn, (C.dpiOracleTypeNum)(vi.Typ), (C.dpiNativeTypeNum)(vi.NatTyp), C.uint32_t(vi.SliceLen),
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

func NewVarInfo(baseType interface{}, sliceLen, bufSize int) (VarInfo, error) {
	var vi VarInfo

	switch v := baseType.(type) {
	case Lob, []Lob:
		vi.NatTyp = C.DPI_NATIVE_TYPE_LOB
		var isClob bool
		switch v := v.(type) {
		case Lob:
			isClob = v.IsClob
		case []Lob:
			isClob = len(v) > 0 && v[0].IsClob
		}
		if isClob {
			vi.Typ = C.DPI_ORACLE_TYPE_CLOB
		} else {
			vi.Typ = C.DPI_ORACLE_TYPE_BLOB
		}
		// FIXME(tgulacsi):
	//case Number, []Number:
	//vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_BYTES
	case int, []int, int64, []int64, sql.NullInt64, []sql.NullInt64:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_INT64
	case int32, []int32:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_NATIVE_TYPE_INT64
	case uint, []uint, uint64, []uint64:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_UINT64
	case uint32, []uint32:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NATIVE_UINT, C.DPI_NATIVE_TYPE_UINT64
	case float32, []float32:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NATIVE_FLOAT, C.DPI_NATIVE_TYPE_FLOAT
	case float64, []float64, sql.NullFloat64, []sql.NullFloat64:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_NATIVE_DOUBLE, C.DPI_NATIVE_TYPE_DOUBLE
	case bool, []bool:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_BOOLEAN, C.DPI_NATIVE_TYPE_BOOLEAN
	case []byte, [][]byte:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_RAW, C.DPI_NATIVE_TYPE_BYTES
		switch v := v.(type) {
		case []byte:
			bufSize = len(v)
		case [][]byte:
			for _, b := range v {
				if n := len(b); n > bufSize {
					bufSize = n
				}
			}
		}
	case string, []string, nil:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_NATIVE_TYPE_BYTES
		bufSize = 32767
	case time.Time, []time.Time:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_DATE, C.DPI_NATIVE_TYPE_TIMESTAMP
	case userType, []userType:
		vi.Typ, vi.NatTyp = C.DPI_ORACLE_TYPE_OBJECT, C.DPI_NATIVE_TYPE_OBJECT
		switch v := v.(type) {
		case userType:
			vi.ObjectType = (*CdpiObjectType)(v.ObjectRef().ObjectType.dpiObjectType)
		case []userType:
			if len(v) > 0 {
				vi.ObjectType = (*CdpiObjectType)(v[0].ObjectRef().ObjectType.dpiObjectType)
			}
		}
	default:
		return vi, errors.Errorf("unknown type %T", v)
	}

	vi.IsPLSArray = reflect.TypeOf(baseType).Kind() == reflect.Slice
	vi.SliceLen = sliceLen
	vi.BufSize = bufSize

	return vi, nil
}

func (d *Data) Reset() {
	if d == nil {
		return
	}
	d.NativeTypeNum = 0
	d.ObjectType = ObjectType{}
	d.SetBytes(nil)
	d.dpiData.isNull = 1
}

var dataPool = dpiDataPool{Pool: &sync.Pool{New: func() interface{} { return &C.dpiData{} }}}

type dpiDataPool struct {
	*sync.Pool
}

func (dp dpiDataPool) Get() *Data {
	d := dp.Pool.Get().(*Data)
	d.Reset()
	return d
}
func (dp dpiDataPool) Put(d *Data) {
	if d == nil {
		return
	}
	d.Reset()
	dp.Pool.Put(d)
}
