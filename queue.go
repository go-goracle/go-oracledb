// Copyright 2019 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0

package oracledb

import (
	"context"
	"sync"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"

	"github.com/go-goracle/go-oracledb/internal"
)

type (
	EnqOptions = internal.EnqOptions
	DeqOptions = internal.DeqOptions

	// MessageState constants representing message's state.
	MessageState = internal.MessageState
	// DeqNavigation constants for navigation.
	DeqNavigation = internal.DeqNavigation

	// DeqMode constants for dequeue modes.
	DeqMode = internal.DeqMode

	// Visibility constants represents visibility.
	Visibility = internal.Visibility

	// DeliveryMode constants for delivery modes.
	DeliveryMode = internal.DeliveryMode

	// EnqOptions are the options used to enqueue a message.
	EnqOptions = internal.EnqOptions
	// DeqOptions are the options used to dequeue a message.
	DeqOptions = internal.DeqOptions
)

// DefaultEnqOptions is the default set for NewQueue.
var DefaultEnqOptions = EnqOptions{
	Visibility:   VisibleImmediate,
	DeliveryMode: DeliverPersistent,
}

// DefaultDeqOptions is the default set for NewQueue.
var DefaultDeqOptions = DeqOptions{
	Mode:         DeqRemove,
	DeliveryMode: DeliverPersistent,
	Navigation:   NavFirst,
	Visibility:   VisibleImmediate,
	Wait:         30,
}

// Queue represents an Oracle Advanced Queue.
type Queue struct {
	//conn              *conn
	iQueue            *internal.Queue
	PayloadObjectType ObjectType
	name              string

	mu sync.Mutex
}

// NewQueue creates a new Queue.
//
// WARNING: the connection given to it must not be closed before the Queue is closed!
// So use an sql.Conn for it.
func NewQueue(ctx context.Context, execer Execer, name string, payloadObjectTypeName string) (*Queue, error) {
	cx, err := DriverConn(ctx, execer)
	if err != nil {
		return nil, err
	}
	//Q := Queue{conn: cx.(*conn)}
	var Q Queue

	if Q.iQueue, err = cx.NewQueue(name, payloadObjectTypeName); err != nil {
		cx.Close()
		return nil, err
	}
	if err = Q.SetEnqOptions(DefaultEnqOptions); err != nil {
		cx.Close()
		Q.Close()
		return nil, err
	}
	if err = Q.SetDeqOptions(DefaultDeqOptions); err != nil {
		cx.Close()
		Q.Close()
		return nil, err
	}
	return &Q, nil
}

// Close the queue.
func (Q *Queue) Close() error {
	//c, q := Q.conn, Q.dpiQueue
	//Q.conn, Q.dpiQueue = nil, nil
	q := Q.iQueue
	Q.iQueue = nil
	if q == nil {
		return nil
	}
	return q.Release()
}

// Name of the queue.
func (Q *Queue) Name() string { return Q.Name() }

// EnqOptions returns the queue's enqueue options in effect.
func (Q *Queue) EnqOptions() (EnqOptions, error) {
	return Q.iQueue.EnqOptions()
}

// DeqOptions returns the queue's dequeue options in effect.
func (Q *Queue) DeqOptions() (DeqOptions, error) {
	return Q.iQueue.DeqOptions()
}

// Dequeues messages into the given slice.
// Returns the number of messages filled in the given slice.
func (Q *Queue) Dequeue(messages []Message) (int, error) {
	Q.mu.Lock()
	defer Q.mu.Unlock()
	return Q.Dequeue(messages)
}

// Enqueue all the messages given.
//
// WARNING: calling this function in parallel on different connections acquired from the same pool may fail due to Oracle bug 29928074. Ensure that this function is not run in parallel, use standalone connections or connections from different pools, or make multiple calls to Queue.enqOne() instead. The function Queue.Dequeue() call is not affected.
func (Q *Queue) Enqueue(messages []Message) error {
	Q.mu.Lock()
	defer Q.mu.Unlock()
	return Q.Enqueue(messages)
}

// SetEnqOptions sets all the enqueue options
func (Q *Queue) SetEnqOptions(E EnqOptions) error {
	var opts *C.dpiEnqOptions
	if C.dpiQueue_getEnqOptions(Q.dpiQueue, &opts) == C.DPI_FAILURE {
		return errors.Errorf("getEnqOptions: %w", Q.conn.drv.getError())
	}
	return E.toOra(Q.conn.drv, opts)
}

// SetDeqOptions sets all the dequeue options
func (Q *Queue) SetDeqOptions(D DeqOptions) error {
	return Q.iQueue.SetDeqOptions(D)
}

// SetDeqCorrelation is a convenience function setting the Correlation DeqOption
func (Q *Queue) SetDeqCorrelation(correlation string) error {
	return Q.iQueue.SetDeqCorrelation(correlation)
}

const (
	NoWait      = internal.NoWait
	WaitForever = internal.WaitForever
)

const (
	VisibleImmediate  = internal.VisibleImmediate
	DeliverPersistent = internal.DeliverPersistent

	// MsgStateReady says that "The message is ready to be processed".
	MsgStateReady = MessageState(C.DPI_MSG_STATE_READY)
	// MsgStateWaiting says that "The message is waiting for the delay time to expire".
	MsgStateWaiting = MessageState(C.DPI_MSG_STATE_WAITING)
	// MsgStateProcessed says that "The message has already been processed and is retained".
	MsgStateProcessed = MessageState(C.DPI_MSG_STATE_PROCESSED)
	// MsgStateExpired says that "The message has been moved to the exception queue".
	MsgStateExpired = MessageState(C.DPI_MSG_STATE_EXPIRED)

	// DeliverPersistent is to Dequeue only persistent messages from the queue. This is the default mode.
	DeliverPersistent = DeliveryMode(C.DPI_MODE_MSG_PERSISTENT)
	// DeliverBuffered is to Dequeue only buffered messages from the queue.
	DeliverBuffered = DeliveryMode(C.DPI_MODE_MSG_BUFFERED)
	// DeliverPersistentOrBuffered is to Dequeue both persistent and buffered messages from the queue.
	DeliverPersistentOrBuffered = DeliveryMode(C.DPI_MODE_MSG_PERSISTENT_OR_BUFFERED)

	// VisibleImmediate means that "The message is not part of the current transaction but constitutes a transaction of its own".
	VisibleImmediate = Visibility(C.DPI_VISIBILITY_IMMEDIATE)
	// VisibleOnCommit means that "The message is part of the current transaction. This is the default value".
	VisibleOnCommit = Visibility(C.DPI_VISIBILITY_ON_COMMIT)
	// DeqRemove reads the message and updates or deletes it. This is the default mode. Note that the message may be retained in the queue table based on retention properties.

	DeqRemove = DeqMode(C.DPI_MODE_DEQ_REMOVE)
	// DeqBrows reads the message without acquiring a lock on the message (equivalent to a SELECT statement).
	DeqBrowse = DeqMode(C.DPI_MODE_DEQ_BROWSE)
	// DeqLocked reads the message and obtain a write lock on the message (equivalent to a SELECT FOR UPDATE statement).
	DeqLocked = DeqMode(C.DPI_MODE_DEQ_LOCKED)
	// DeqPeek confirms receipt of the message but does not deliver the actual message content.
	DeqPeek = DeqMode(C.DPI_MODE_DEQ_REMOVE_NO_DATA)

	// NavFirst retrieves the first available message that matches the search criteria. This resets the position to the beginning of the queue.
	NavFirst = DeqNavigation(C.DPI_DEQ_NAV_FIRST_MSG)
	// NavNext skips the remainder of the current transaction group (if any) and retrieves the first message of the next transaction group. This option can only be used if message grouping is enabled for the queue.
	NavNextTran = DeqNavigation(C.DPI_DEQ_NAV_NEXT_TRANSACTION)
	// NavNext  	Retrieves the next available message that matches the search criteria. This is the default method.
	NavNext = DeqNavigation(C.DPI_DEQ_NAV_NEXT_MSG)
)
