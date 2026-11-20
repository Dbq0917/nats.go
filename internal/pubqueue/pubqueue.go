// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package pubqueue provides a bounded queue for messages that have been
// handed to an asynchronous publisher but have not been written to the
// underlying connection yet.
//
// The capacity is fixed when the queue is created and can never be grown.
// Callers choose how a full queue is handled:
//
//   - Push rejects the message with ErrQueueFull instead of dropping it
//     silently, so the caller stays in control of back-pressure.
//   - PushWait blocks until a slot becomes available, but honors a context,
//     so a stalled connection can never hang the calling goroutine forever.
//
// Both entry points send into a buffered channel. The non-blocking fast
// path performs no heap allocation beyond the value being enqueued.
package pubqueue

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrQueueFull is returned by Push when the queue has no free slot.
	// The message has not been enqueued and remains owned by the caller.
	ErrQueueFull = errors.New("nats: publish queue full")

	// ErrQueueClosed is returned when the queue has been closed.
	ErrQueueClosed = errors.New("nats: publish queue closed")
)

// DefaultPendingCapacity is the fixed number of messages an asynchronous
// publisher will keep waiting for the background writer once a connection is
// established. It is chosen to absorb normal publish bursts; publishers that
// exceed it get explicit back-pressure instead of silent drops.
const DefaultPendingCapacity = 10000

// Item is the payload queued for a background writer. It keeps the fields
// the publisher needs to write a protocol message without forcing callers to
// box arguments in an allocation of their own.
type Item struct {
	Subject string
	Reply   string
	Hdr     []byte
	Data    []byte
}

// Queue is a fixed-capacity, concurrency-safe FIFO of pending publish items.
// It is backed by a single buffered channel. A mutex only protects the closed
// flag flip; channel sends happen without the lock held, so Close can never
// cause a send-on-closed-channel panic and a blocked PushWait never wedges
// the consumer.
type Queue struct {
	mu     sync.Mutex
	items  chan Item
	done   chan struct{}
	cap    int
	closed bool
}

// New creates a queue with the given fixed capacity. A capacity below one is
// rejected so the queue can always accept at least one item.
func New(capacity int) (*Queue, error) {
	if capacity < 1 {
		return nil, errors.New("nats: publish queue capacity must be >= 1")
	}
	return &Queue{
		items: make(chan Item, capacity),
		done:  make(chan struct{}),
		cap:   capacity,
	}, nil
}

// Push enqueues item without waiting. It returns ErrQueueFull when every
// slot is occupied and ErrQueueClosed after Close. The message is never
// dropped on the caller's behalf: a full queue always surfaces an error.
//
// The fast path is a single non-blocking channel send and allocates nothing
// beyond item itself.
func (q *Queue) Push(item Item) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	done := q.done
	q.mu.Unlock()

	// A non-blocking send never parks, so a concurrent Close cannot close the
	// channel while this send is in flight.
	select {
	case q.items <- item:
		return nil
	case <-done:
		return ErrQueueClosed
	default:
		return ErrQueueFull
	}
}

// PushWait enqueues item, waiting for a free slot when the queue is full.
// It returns nil once the item is safely queued.
//
// Waiting is bounded by ctx: cancelling the context (or its deadline
// expiring) unblocks the caller with the context's error rather than hanging
// the goroutine on a connection that has stopped draining. ErrQueueClosed is
// returned if the queue is closed while waiting.
func (q *Queue) PushWait(ctx context.Context, item Item) error {
	if ctx == nil {
		return errors.New("nats: context required for blocking publish")
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	done := q.done
	q.mu.Unlock()

	// Try the non-blocking fast path first so the common case never parks on
	// the context.
	select {
	case q.items <- item:
		return nil
	default:
	}

	// Full: wait for a slot, a close, or context cancellation. The consumer
	// keeps draining q.items without taking the lock, so a stalled connection
	// cannot wedge this goroutine as long as the context is bounded.
	select {
	case q.items <- item:
		return nil
	case <-done:
		return ErrQueueClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pop blocks until an item is available or the queue is closed and empty.
// The boolean is false when no more items will ever be delivered, signaling
// the writer goroutine to exit.
func (q *Queue) Pop() (Item, bool) {
	for {
		// Prefer a buffered item so nothing is lost on a closed queue.
		select {
		case item := <-q.items:
			return item, true
		default:
		}

		select {
		case item, ok := <-q.items:
			if ok {
				return item, true
			}
			return Item{}, false
		case <-q.done:
			// Closed: keep draining until the buffer is empty.
			select {
			case item := <-q.items:
				return item, true
			default:
				return Item{}, false
			}
		}
	}
}

// Close stops the queue from accepting new items. Items already queued remain
// readable via Pop until drained. Closing an already-closed queue is a no-op.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		// Signals any parked producer without closing the items channel,
		// which would risk a send-on-closed-channel panic.
		close(q.done)
	}
}

// Len returns the number of items currently waiting.
func (q *Queue) Len() int { return len(q.items) }

// Cap returns the fixed capacity the queue was created with.
func (q *Queue) Cap() int { return q.cap }

// Closed reports whether Close has been called.
func (q *Queue) Closed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}
