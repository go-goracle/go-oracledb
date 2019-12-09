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
	"reflect"
	"strings"
	"unsafe"

	errors "golang.org/x/xerrors"
)

var _ = fmt.Printf

// Object represents a dpiObject.
type Object struct {
	ObjectType
	dpiObject *C.dpiObject
}

func (O *Object) Err() error { return O.Conn.Err() }

// ErrNoSuchKey is the error for missing key in lookup.
var ErrNoSuchKey = errors.New("no such key")

// GetAttribute gets the i-th attribute into data.
func (O *Object) GetAttribute(data *Data, name string) error {
	if O == nil || O.dpiObject == nil {
		panic("nil dpiObject")
	}
	attr, ok := O.Attributes[name]
	if !ok {
		return errors.Errorf("%s: %w", name, ErrNoSuchKey)
	}

	data.Reset()
	if data == nil {
		data = &Data{}
	}
	data.NativeTypeNum = attr.NativeTypeNum
	data.ObjectType = attr.ObjectType
	// the maximum length of that buffer must be supplied
	// in the value.asBytes.length attribute before calling this function.
	if attr.NativeTypeNum == C.DPI_NATIVE_TYPE_BYTES && attr.OracleTypeNum == C.DPI_ORACLE_TYPE_NUMBER {
		var a [39]byte
		C.dpiData_setBytes(&data.dpiData, (*C.char)(unsafe.Pointer(&a[0])), C.uint32_t(len(a)))
	}

	//fmt.Printf("getAttributeValue(%p, %p, %d, %+v)\n", O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData)
	if C.dpiObject_getAttributeValue(O.dpiObject, attr.dpiObjectAttr, C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
		return errors.Errorf("getAttributeValue(%q, obj=%+v, attr=%+v, typ=%d): %w", name, O, attr.dpiObjectAttr, data.NativeTypeNum, O.Err())
	}
	//fmt.Printf("getAttributeValue(%p, %q=%p, %d, %+v)\n", O.dpiObject, attr.Name, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData)
	return nil
}

// SetAttribute sets the named attribute with data.
func (O *Object) SetAttribute(name string, data *Data) error {
	if !strings.Contains(name, `"`) {
		name = strings.ToUpper(name)
	}
	attr := O.Attributes[name]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
		data.ObjectType = attr.ObjectType
	}
	if C.dpiObject_setAttributeValue(O.dpiObject, attr.dpiObjectAttr, C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
		return O.Err()
	}
	return nil
}

// Set is a convenience function to set the named attribute with the given value.
func (O *Object) Set(name string, v interface{}) error {
	if data, ok := v.(*Data); ok {
		return O.SetAttribute(name, data)
	}
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	if err := scratch.Set(v); err != nil {
		return err
	}
	return O.SetAttribute(name, scratch)
}

// ResetAttributes prepare all attributes for use the object as IN parameter
func (O *Object) ResetAttributes() error {
	var data Data
	for _, attr := range O.Attributes {
		data.Reset()
		data.NativeTypeNum = attr.NativeTypeNum
		data.ObjectType = attr.ObjectType
		if attr.NativeTypeNum == C.DPI_NATIVE_TYPE_BYTES && attr.OracleTypeNum == C.DPI_ORACLE_TYPE_NUMBER {
			a := make([]byte, attr.Precision)
			C.dpiData_setBytes(&data.dpiData, (*C.char)(unsafe.Pointer(&a[0])), C.uint32_t(attr.Precision))
		}
		if C.dpiObject_setAttributeValue(O.dpiObject, attr.dpiObjectAttr, C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
			return O.Err()
		}
	}

	return nil
}

// Get scans the named attribute into dest, and returns it.
func (O *Object) Get(name string) (interface{}, error) {
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	if err := O.GetAttribute(scratch, name); err != nil {
		return nil, err
	}
	isObject := scratch.IsObject()
	if isObject {
		scratch.ObjectType = O.Attributes[name].ObjectType
	}
	v := scratch.Get()
	if !isObject {
		return v, nil
	}
	sub := v.(*Object)
	if sub != nil && sub.CollectionOf != nil {
		return &ObjectCollection{Object: sub}, nil
	}
	return sub, nil
}

// ObjectRef implements userType interface.
func (O *Object) ObjectRef() *Object {
	return O
}

// Collection returns &ObjectCollection{Object: O} iff the Object is a collection.
// Otherwise it returns nil.
func (O *Object) Collection() *ObjectCollection {
	if O.ObjectType.CollectionOf == nil {
		return nil
	}
	return &ObjectCollection{Object: O}
}

// Close releases a reference to the object.
func (O *Object) Close() error {
	obj := O.dpiObject
	O.dpiObject = nil
	if obj == nil {
		return nil
	}
	if C.dpiObject_release(obj) == C.DPI_FAILURE {
		return errors.Errorf("error on close object: %w", O.Err())
	}

	return nil
}

// ObjectCollection represents a Collection of Objects - itself an Object, too.
type ObjectCollection struct {
	*Object
}

// ErrNotCollection is returned when the Object is not a collection.
var ErrNotCollection = errors.New("not collection")

// ErrNotExist is returned when the collection's requested element does not exist.
var ErrNotExist = errors.New("not exist")

// AsSlice retrieves the collection into a slice.
func (O *ObjectCollection) AsSlice(dest interface{}) (interface{}, error) {
	var dr reflect.Value
	needsInit := dest == nil
	if !needsInit {
		dr = reflect.ValueOf(dest)
	}
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	for i, err := O.First(); err == nil; i, err = O.Next(i) {
		if O.CollectionOf.NativeTypeNum == C.DPI_NATIVE_TYPE_OBJECT {
			scratch.ObjectType = *O.CollectionOf
		}
		if err = O.GetItem(scratch, i); err != nil {
			return dest, err
		}
		vr := reflect.ValueOf(scratch.Get())
		if needsInit {
			needsInit = false
			length, lengthErr := O.Len()
			if lengthErr != nil {
				return dr.Interface(), lengthErr
			}
			dr = reflect.MakeSlice(reflect.SliceOf(vr.Type()), 0, length)
		}
		dr = reflect.Append(dr, vr)
	}
	return dr.Interface(), nil
}

// AppendData to the collection.
func (O *ObjectCollection) AppendData(data *Data) error {
	if C.dpiObject_appendElement(O.dpiObject, C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
		return errors.Errorf("append(%d): %w", data.NativeTypeNum, O.Err())
	}
	return nil
}

// Append v to the collection.
func (O *ObjectCollection) Append(v interface{}) error {
	if data, ok := v.(*Data); ok {
		return O.AppendData(data)
	}
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	if err := scratch.Set(v); err != nil {
		return err
	}
	return O.AppendData(scratch)
}

// AppendObject adds an Object to the collection.
func (O *ObjectCollection) AppendObject(obj *Object) error {
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	*scratch = Data{
		ObjectType:    obj.ObjectType,
		NativeTypeNum: CdpiNativeTypeNum(C.DPI_NATIVE_TYPE_OBJECT),
		dpiData:       C.dpiData{isNull: 1},
	}
	scratch.SetObject(obj)
	return O.Append(scratch)
}

// Delete i-th element of the collection.
func (O *ObjectCollection) Delete(i int) error {
	if C.dpiObject_deleteElementByIndex(O.dpiObject, C.int32_t(i)) == C.DPI_FAILURE {
		return errors.Errorf("delete(%d): %w", i, O.Err())
	}
	return nil
}

// GetItem gets the i-th element of the collection into data.
func (O *ObjectCollection) GetItem(data *Data, i int) error {
	if data == nil {
		panic("data cannot be nil")
	}
	idx := C.int32_t(i)
	var exists C.int
	if C.dpiObject_getElementExistsByIndex(O.dpiObject, idx, &exists) == C.DPI_FAILURE {
		return errors.Errorf("exists(%d): %w", idx, O.Err())
	}
	if exists == 0 {
		return ErrNotExist
	}
	data.Reset()
	data.NativeTypeNum = O.CollectionOf.NativeTypeNum
	data.ObjectType = *O.CollectionOf
	if C.dpiObject_getElementValueByIndex(O.dpiObject, idx, C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
		return errors.Errorf("get(%d[%d]): %w", idx, data.NativeTypeNum, O.Err())
	}
	return nil
}

// Get the i-th element of the collection.
func (O *ObjectCollection) Get(i int) (interface{}, error) {
	var data Data
	err := O.GetItem(&data, i)
	return data.Get(), err
}

// SetItem sets the i-th element of the collection with data.
func (O *ObjectCollection) SetItem(i int, data *Data) error {
	if C.dpiObject_setElementValueByIndex(O.dpiObject, C.int32_t(i), C.uint(data.NativeTypeNum), &data.dpiData) == C.DPI_FAILURE {
		return errors.Errorf("set(%d[%d]): %w", i, data.NativeTypeNum, O.Err())
	}
	return nil
}

// Set the i-th element of the collection with value.
func (O *ObjectCollection) Set(i int, v interface{}) error {
	if data, ok := v.(*Data); ok {
		return O.SetItem(i, data)
	}
	scratch := dataPool.Get()
	defer dataPool.Put(scratch)
	if err := scratch.Set(v); err != nil {
		return err
	}
	return O.SetItem(i, scratch)
}

// First returns the first element's index of the collection.
func (O *ObjectCollection) First() (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getFirstIndex(O.dpiObject, &idx, &exists) == C.DPI_FAILURE {
		return 0, errors.Errorf("first: %w", O.Err())
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}

// Last returns the index of the last element.
func (O *ObjectCollection) Last() (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getLastIndex(O.dpiObject, &idx, &exists) == C.DPI_FAILURE {
		return 0, errors.Errorf("last: %w", O.Err())
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}

// Next returns the succeeding index of i.
func (O *ObjectCollection) Next(i int) (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getNextIndex(O.dpiObject, C.int32_t(i), &idx, &exists) == C.DPI_FAILURE {
		return 0, errors.Errorf("next(%d): %w", i, O.Err())
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}

// Len returns the length of the collection.
func (O *ObjectCollection) Len() (int, error) {
	var size C.int32_t
	if C.dpiObject_getSize(O.dpiObject, &size) == C.DPI_FAILURE {
		return 0, errors.Errorf("len: %w", O.Err())
	}
	return int(size), nil
}

// Trim the collection to n.
func (O *ObjectCollection) Trim(n int) error {
	if C.dpiObject_trim(O.dpiObject, C.uint32_t(n)) == C.DPI_FAILURE {
		return O.Err()
	}
	return nil
}

// ObjectType holds type info of an Object.
type ObjectType struct {
	dpiObjectType *C.dpiObjectType
	Conn          *Conn

	Schema, Name                        string
	DBSize, ClientSizeInBytes, CharSize int
	CollectionOf                        *ObjectType
	Attributes                          map[string]ObjectAttribute
	OracleTypeNum                       CdpiOracleTypeNum
	NativeTypeNum                       CdpiNativeTypeNum
	Precision                           int16
	Scale                               int8
	FsPrecision                         uint8
}

func (t ObjectType) Err() error { return t.Conn.Err() }

func (t ObjectType) String() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// FullName returns the object's name with the schame prepended.
func (t ObjectType) FullName() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// GetObjectType returns the ObjectType of a name.
//
// The name is uppercased! Because here Oracle seems to be case-sensitive.
// To leave it as is, enclose it in "-s!
func (c *Conn) GetObjectType(name string) (ObjectType, error) {
	if !strings.Contains(name, "\"") {
		name = strings.ToUpper(name)
	}
	cName := C.CString(name)
	defer func() { C.free(unsafe.Pointer(cName)) }()
	objType := (*C.dpiObjectType)(C.malloc(C.sizeof_void))
	if C.dpiConn_getObjectType(c.dpiConn, cName, C.uint32_t(len(name)), &objType) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(objType))
		return ObjectType{}, errors.Errorf("getObjectType(%q) conn=%p: %w", name, c.dpiConn, c.Err())
	}
	t := ObjectType{Conn: c, dpiObjectType: objType}
	return t, t.init()
}

// NewObject returns a new Object with ObjectType type.
//
// As with all Objects, you MUST call Close on it when not needed anymore!
func (t ObjectType) NewObject() (*Object, error) {
	obj := (*C.dpiObject)(C.malloc(C.sizeof_void))
	if C.dpiObjectType_createObject(t.dpiObjectType, &obj) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(obj))
		return nil, t.Err()
	}
	O := &Object{ObjectType: t, dpiObject: obj}
	// https://github.com/oracle/odpi/issues/112#issuecomment-524479532
	return O, O.ResetAttributes()
}

// NewCollection returns a new Collection object with ObjectType type.
// If the ObjectType is not a Collection, it returns ErrNotCollection error.
func (t ObjectType) NewCollection() (*ObjectCollection, error) {
	if t.CollectionOf == nil {
		return nil, ErrNotCollection
	}
	O, err := t.NewObject()
	if err != nil {
		return nil, err
	}
	return &ObjectCollection{Object: O}, nil
}

// Close releases a reference to the object type.
func (t *ObjectType) close() error {
	if t == nil {
		return nil
	}
	attributes, d := t.Attributes, t.dpiObjectType
	t.Attributes, t.dpiObjectType = nil, nil

	for _, attr := range attributes {
		err := attr.Close()
		if err != nil {
			return err
		}
	}

	if d == nil {
		return nil
	}

	if C.dpiObjectType_release(d) == C.DPI_FAILURE {
		return errors.Errorf("error on close object type: %w", t.Err())
	}

	return nil
}

func wrapObject(c *Conn, objectType *C.dpiObjectType, object *C.dpiObject) (*Object, error) {
	if objectType == nil {
		return nil, errors.New("objectType is nil")
	}
	if C.dpiObject_addRef(object) == C.DPI_FAILURE {
		return nil, c.Err()
	}
	o := &Object{
		ObjectType: ObjectType{dpiObjectType: objectType, Conn: c},
		dpiObject:  object,
	}
	return o, o.init()
}

func (t *ObjectType) init() error {
	if t.Conn == nil {
		panic("conn is nil")
	}
	if t.Name != "" && t.Attributes != nil {
		return nil
	}
	if t.dpiObjectType == nil {
		return nil
	}
	var info C.dpiObjectTypeInfo
	if C.dpiObjectType_getInfo(t.dpiObjectType, &info) == C.DPI_FAILURE {
		return errors.Errorf("%v.getInfo: %w", t, t.Err())
	}
	t.Schema = C.GoStringN(info.schema, C.int(info.schemaLength))
	t.Name = C.GoStringN(info.name, C.int(info.nameLength))
	t.CollectionOf = nil
	numAttributes := int(info.numAttributes)

	if info.isCollection == 1 {
		t.CollectionOf = &ObjectType{Conn: t.Conn}
		if err := t.CollectionOf.fromDataTypeInfo(info.elementTypeInfo); err != nil {
			return err
		}
		if t.CollectionOf.Name == "" {
			t.CollectionOf.Schema = t.Schema
			t.CollectionOf.Name = t.Name
		}
	}
	if numAttributes == 0 {
		t.Attributes = map[string]ObjectAttribute{}
		return nil
	}
	t.Attributes = make(map[string]ObjectAttribute, numAttributes)
	attrs := make([]*C.dpiObjectAttr, numAttributes)
	if C.dpiObjectType_getAttributes(t.dpiObjectType,
		C.uint16_t(len(attrs)),
		(**C.dpiObjectAttr)(unsafe.Pointer(&attrs[0])),
	) == C.DPI_FAILURE {
		return errors.Errorf("%v.getAttributes: %w", t, t.Err())
	}
	for _, attr := range attrs {
		var attrInfo C.dpiObjectAttrInfo
		if C.dpiObjectAttr_getInfo(attr, &attrInfo) == C.DPI_FAILURE {
			return errors.Errorf("%v.attr_getInfo: %w", attr, t.Err())
		}
		typ := attrInfo.typeInfo
		sub, err := objectTypeFromDataTypeInfo(t.Conn, typ)
		if err != nil {
			return err
		}
		objAttr := ObjectAttribute{
			dpiObjectAttr: attr,
			Name:          C.GoStringN(attrInfo.name, C.int(attrInfo.nameLength)),
			ObjectType:    sub,
		}
		//fmt.Printf("%d=%q. typ=%+v sub=%+v\n", i, objAttr.Name, typ, sub)
		t.Attributes[objAttr.Name] = objAttr
	}
	return nil
}

func (t *ObjectType) fromDataTypeInfo(typ C.dpiDataTypeInfo) error {
	t.dpiObjectType = typ.objectType

	t.OracleTypeNum = CdpiOracleTypeNum(typ.oracleTypeNum)
	t.NativeTypeNum = CdpiNativeTypeNum(typ.defaultNativeTypeNum)
	t.DBSize = int(typ.dbSizeInBytes)
	t.ClientSizeInBytes = int(typ.clientSizeInBytes)
	t.CharSize = int(typ.sizeInChars)
	t.Precision = int16(typ.precision)
	t.Scale = int8(typ.scale)
	t.FsPrecision = uint8(typ.fsPrecision)
	return t.init()
}
func objectTypeFromDataTypeInfo(conn *Conn, typ C.dpiDataTypeInfo) (ObjectType, error) {
	if conn == nil {
		panic("conn is nil")
	}
	if typ.oracleTypeNum == 0 {
		panic("typ is nil")
	}
	t := ObjectType{Conn: conn}
	err := t.fromDataTypeInfo(typ)
	return t, err
}

// ObjectAttribute is an attribute of an Object.
type ObjectAttribute struct {
	Name          string
	dpiObjectAttr *C.dpiObjectAttr
	ObjectType
}

// Close the ObjectAttribute.
func (A *ObjectAttribute) Close() error {
	attr := A.dpiObjectAttr
	A.dpiObjectAttr = nil

	if attr == nil {
		return nil
	}
	if C.dpiObjectAttr_release(attr) == C.DPI_FAILURE {
		return A.Err()
	}
	return nil
}

type userType interface {
	ObjectRef() *Object
}
