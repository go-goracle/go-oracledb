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
	"fmt"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"
)

var ErrNotSupported = errors.New("not supported")

// NewData creates a new Data structure for the given type, populated with the given type.
func NewData() *CdpiData {
	return &CdpiData{isNull: 1}
}

// IsNull returns whether the data is null.
func (d *CdpiData) IsNull() bool {
	// Use of C.dpiData_getIsNull(d) would be safer,
	// but ODPI-C 3.1.4 just returns dpiData->isNull, so do the same
	// without calling CGO.
	return d == nil || d.isNull == 1
}

// SetNull sets the value of the data to be the null value.
func (d *CdpiData) SetNull() {
	if !d.IsNull() {
		// Maybe C.dpiData_setNull(d) would be safer, but as we don't use C.dpiData_getIsNull,
		// and those functions (at least in ODPI-C 3.1.4) just operate on data->isNull directly,
		// don't use CGO if possible.
		d.isNull = 1
	}
}

// GetBool returns the bool data.
func (d *CdpiData) GetBool() bool {
	return !d.IsNull() && C.dpiData_getBool((*C.dpiData)(d)) == 1
}

// SetBool sets the data as bool.
func (d *CdpiData) SetBool(b bool) {
	var i C.int
	if b {
		i = 1
	}
	C.dpiData_setBool((*C.dpiData)(d), i)
}

// GetBytes returns the []byte from the data.
func (d *CdpiData) GetBytes() []byte {
	if d.IsNull() {
		return nil
	}
	b := C.dpiData_getBytes((*C.dpiData)(d))
	if b.ptr == nil || b.length == 0 {
		return nil
	}
	return ((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length]
}

// SetBytes set the data as []byte.
func (d *CdpiData) SetBytes(b []byte) {
	if b == nil {
		d.isNull = 1
		return
	}
	C.dpiData_setBytes((*C.dpiData)(d), (*C.char)(unsafe.Pointer(&b[0])), C.uint32_t(len(b)))
}

// GetFloat32 gets float32 from the data.
func (d *CdpiData) GetFloat32() float32 {
	if d.IsNull() {
		return 0
	}
	return float32(C.dpiData_getFloat((*C.dpiData)(d)))
}

// SetFloat32 sets the data as float32.
func (d *CdpiData) SetFloat32(f float32) {
	C.dpiData_setFloat((*C.dpiData)(d), C.float(f))
}

// GetFloat64 gets float64 from the data.
func (d *CdpiData) GetFloat64() float64 {
	//fmt.Println("GetFloat64", d.IsNull(), d)
	if d.IsNull() {
		return 0
	}
	return float64(C.dpiData_getDouble((*C.dpiData)(d)))
}

// SetFloat64 sets the data as float64.
func (d *CdpiData) SetFloat64(f float64) {
	C.dpiData_setDouble((*C.dpiData)(d), C.double(f))
}

// GetInt64 gets int64 from the data.
func (d *CdpiData) GetInt64() int64 {
	if d.IsNull() {
		return 0
	}
	return int64(C.dpiData_getInt64((*C.dpiData)(d)))
}

// SetInt64 sets the data as int64.
func (d *CdpiData) SetInt64(i int64) {
	C.dpiData_setInt64((*C.dpiData)(d), C.int64_t(i))
}

// GetIntervalDS gets duration as interval date-seconds from data.
func (d *CdpiData) GetIntervalDS() time.Duration {
	if d.IsNull() {
		return 0
	}
	ds := C.dpiData_getIntervalDS((*C.dpiData)(d))
	return time.Duration(ds.days)*24*time.Hour +
		time.Duration(ds.hours)*time.Hour +
		time.Duration(ds.minutes)*time.Minute +
		time.Duration(ds.seconds)*time.Second +
		time.Duration(ds.fseconds)
}

// SetIntervalDS sets the duration as interval date-seconds to data.
func (d *CdpiData) SetIntervalDS(dur time.Duration) {
	C.dpiData_setIntervalDS((*C.dpiData)(d),
		C.int32_t(int64(dur.Hours())/24),
		C.int32_t(int64(dur.Hours())%24), C.int32_t(dur.Minutes()), C.int32_t(dur.Seconds()),
		C.int32_t(dur.Nanoseconds()),
	)
}

// GetIntervalYM gets IntervalYM from the data.
func (d *CdpiData) GetIntervalYM() IntervalYM {
	if d.IsNull() {
		return IntervalYM{}
	}
	ym := C.dpiData_getIntervalYM((*C.dpiData)(d))
	return IntervalYM{Years: int(ym.years), Months: int(ym.months)}
}

// SetIntervalYM sets IntervalYM to the data.
func (d *CdpiData) SetIntervalYM(ym IntervalYM) {
	C.dpiData_setIntervalYM((*C.dpiData)(d), C.int32_t(ym.Years), C.int32_t(ym.Months))
}

// GetLob gets data as Lob.
func (d *CdpiData) GetLob() *CdpiLob {
	if d.IsNull() {
		return nil
	}
	return (*CdpiLob)(C.dpiData_getLOB((*C.dpiData)(d)))
}

// SetLob sets Lob to the data.
func (d *CdpiData) SetLob(lob *CdpiLob) {
	C.dpiData_setLOB((*C.dpiData)(d), (*C.dpiLob)(lob))
}

// GetObject gets Object from data.
//
// As with all Objects, you MUST call Close on it when not needed anymore!
func (d *CdpiData) GetObject() *CdpiObject {
	if d == nil {
		panic("null")
	}
	if d.IsNull() {
		return nil
	}

	o := C.dpiData_getObject((*C.dpiData)(d))
	if o == nil {
		return nil
	}
	C.dpiObject_addRef(o)
	return (*CdpiObject)(o)
}

// SetObject sets Object to data.
func (d *CdpiData) SetObject(o *CdpiObject) {
	C.dpiData_setObject((*C.dpiData)(d), (*C.dpiObject)(o))
}

// GetStmt gets Stmt from data.
func (d *CdpiData) GetStmt() *CdpiStmt {
	if d.IsNull() {
		return nil
	}
	return (*CdpiStmt)(C.dpiData_getStmt((*C.dpiData)(d)))
}

// SetStmt sets Stmt to data.
func (d *CdpiData) SetStmt(s *CdpiStmt) {
	C.dpiData_setStmt((*C.dpiData)(d), (*C.dpiStmt)(s))
}

// GetTime gets Time from data.
func (d *CdpiData) GetTime() time.Time {
	if d.IsNull() {
		return time.Time{}
	}
	ts := C.dpiData_getTimestamp((*C.dpiData)(d))
	return time.Date(
		int(ts.year), time.Month(ts.month), int(ts.day),
		int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
		timeZoneFor(ts.tzHourOffset, ts.tzMinuteOffset),
	)

}

// SetTime sets Time to data.
func (d *CdpiData) SetTime(t time.Time) {
	_, z := t.Zone()
	C.dpiData_setTimestamp((*C.dpiData)(d),
		C.int16_t(t.Year()), C.uint8_t(t.Month()), C.uint8_t(t.Day()),
		C.uint8_t(t.Hour()), C.uint8_t(t.Minute()), C.uint8_t(t.Second()), C.uint32_t(t.Nanosecond()),
		C.int8_t(z/3600), C.int8_t((z%3600)/60),
	)
}

// GetUint64 gets data as uint64.
func (d *CdpiData) GetUint64() uint64 {
	if d.IsNull() {
		return 0
	}
	return uint64(C.dpiData_getUint64((*C.dpiData)(d)))
}

// SetUint64 sets data to uint64.
func (d *CdpiData) SetUint64(u uint64) {
	C.dpiData_setUint64((*C.dpiData)(d), C.uint64_t(u))
}

// IntervalYM holds Years and Months as interval.
type IntervalYM struct {
	Years, Months int
}

// Get returns the contents of Data.
func (d *CdpiData) Get(nativeType CdpiNativeTypeNum) interface{} {
	switch nativeType {
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
		panic(fmt.Sprintf("unknown NativeTypeNum=%d", nativeType))
	}
}
