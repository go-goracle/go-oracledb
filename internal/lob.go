// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package internal

/*
#include "dpiImpl.h"
*/
import "C"
import (
	//"fmt"
	"unsafe"

	errors "golang.org/x/xerrors"
)

type Lob struct {
	dpiContext *CdpiContext
	dpiLob     *C.dpiLob
}

func (lob *Lob) ReadAt(p []byte, off int64) (int, error) {
	if lob == nil || lob.dpiLob == nil {
		return 0, errors.New("read on nil lob")
	}
	if len(p) == 0 {
		return 0, nil
	}
	n := C.uint64_t(len(p))
	if C.dpiLob_readBytes((*C.dpiLob)(lob.dpiLob), C.uint64_t(off)+1, n, (*C.char)(unsafe.Pointer(&p[0])), &n) == C.DPI_FAILURE {
		C.dpiLob_close((*C.dpiLob)(lob.dpiLob))
		lob.dpiLob = nil
		return int(n), lob.dpiContext.Err()
	}
	return int(n), nil
}

func (lob *Lob) WriteAt(p []byte, off int64) (int, error) {
	//if C.dpiLob_openResource(lob) == C.DPI_FAILURE {

	n := C.uint64_t(len(p))
	if C.dpiLob_writeBytes((*C.dpiLob)(lob.dpiLob), C.uint64_t(off)+1, (*C.char)(unsafe.Pointer(&p[0])), n) == C.DPI_FAILURE {
		C.dpiLob_closeResource((*C.dpiLob)(lob.dpiLob))
		lob.dpiLob = nil
		return int(n), lob.dpiContext.Err()
	}

	return int(n), nil
}

func (lob *Lob) Close() error {
	if lob == nil || lob.dpiLob == nil {
		return nil
	}
	lob.dpiLob = nil
	if C.dpiLob_closeResource((*C.dpiLob)(lob.dpiLob)) == C.DPI_FAILURE {
		err := lob.dpiContext.Err()
		if err.Code() == 22289 { // cannot perform %s operation on an unopened file or LOB
			return nil
		}
		return err
	}
	return nil
}

// NewTempLob returns a temporary LOB as DirectLob.
func (c *Conn) NewTempLob(isClob bool) (*Lob, error) {
	typ := C.uint(C.DPI_ORACLE_TYPE_BLOB)
	if isClob {
		typ = C.DPI_ORACLE_TYPE_CLOB
	}
	var dpiLob *C.dpiLob
	if C.dpiConn_newTempLob((*C.dpiConn)(c.Conn), typ, (**C.dpiLob)(unsafe.Pointer(&dpiLob))) == C.DPI_FAILURE {
		return nil, c.Err()
	}
	return &Lob{dpiLob: dpiLob, dpiContext: c.dpiContext}, nil
}

// Size returns the size of the LOB.
func (lob *Lob) Size() (int64, error) {
	var n C.uint64_t
	if C.dpiLob_getSize((*C.dpiLob)(lob.dpiLob), &n) == C.DPI_FAILURE {
		return int64(n), lob.dpiContext.Err()
	}
	return int64(n), nil
}

// Trim the LOB to the given size.
func (lob *Lob) Trim(size int64) error {
	if C.dpiLob_trim((*C.dpiLob)(lob.dpiLob), C.uint64_t(size)) == C.DPI_FAILURE {
		return lob.dpiContext.Err()
	}
	return nil
}

// Set the contents of the LOB to the given byte slice.
// The LOB is cleared first.
func (lob *Lob) Set(p []byte) error {
	if C.dpiLob_setFromBytes((*C.dpiLob)(lob.dpiLob), (*C.char)(unsafe.Pointer(&p[0])), C.uint64_t(len(p))) == C.DPI_FAILURE {
		return lob.dpiContext.Err()
	}
	return nil
}
