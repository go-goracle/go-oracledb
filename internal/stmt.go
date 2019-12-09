// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package internal

/*
#include <stdlib.h>
#include "dpiImpl.h"

void goracle_setFromString(dpiVar *dv, uint32_t pos, const _GoString_ value) {
	uint32_t length;
	length = _GoStringLen(value);
	if( length == 0 ) {
		return;
	}
	dpiVar_setFromBytes(dv, pos, _GoStringPtr(value), length);
}
*/
import "C"
