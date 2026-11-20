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

package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/pubqueue"
)

// asyncPublisher is a minimal asynchronous publisher backed by pubqueue.
// Publish calls only enqueue; a single writer goroutine drains the queue onto
// the real connection. The queue capacity is fixed at construction.
type asyncPublisher struct {
	nc  *nats.Conn
	q   *pubqueue.Queue
	wg  sync.WaitGroup
	sub string
}

func newAsyncPublisher(t *testing.T, nc *nats.Conn, subject string, capacity int) *asyncPublisher {
	t.Helper()
	q, err := pubqueue.New(capacity)
	if err != nil {
		t.Fatalf("pubqueue.New: %v", err)
	}
	p := &asyncPublisher{nc: nc, q: q, sub: subject}
	p.wg.Add(1)
	go p.writeLoop()
	return p
}

func (p *asyncPublisher) writeLoop() {
	defer p.wg.Done()
	for {
		item, ok := p.q.Pop()
		if !ok {
			return
		}
		// Publish failure (e.g. connection closed) terminates the loop; tests
		// keep the server up so this path is not hit during the happy path.
		if err := p.nc.Publish(item.Subject, item.Data); err != nil {
			return
		}
	}
}

func (p *asyncPublisher) close() {
	p.q.Close()
	p.wg.Wait()
}

// countReceiver collects every message delivered on subject and returns the
// total once the publisher has flushed and drained.
func countReceiver(t *testing.T, nc *nats.Conn, subject string) (*nats.Subscription, func() int) {
	t.Helper()
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var (
		mu       sync.Mutex
		received int
		stopOnce sync.Once
	)
	done := make(chan struct{})
	go func() {
		for {
			msg, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			if msg == nil {
				continue
			}
			mu.Lock()
			received++
			mu.Unlock()
		}
	}()
	stop := func() int {
		stopOnce.Do(func() { close(done) })
		mu.Lock()
		defer mu.Unlock()
		return received
	}
	return sub, stop
}

// TestPubQueueFullReportsError verifies that a saturated queue rejects
// overflow with ErrQueueFull instead of silently dropping the message.
func TestPubQueueFullReportsError(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		const subject = "pq.full"
		p := newAsyncPublisher(t, nc, subject, 4)

		// Fill the queue without letting the writer drain it. The writer
		// goroutine will race with us, so retry until we observe a full queue.
		filled := 0
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			err := p.q.Push(pubqueue.Item{Subject: subject, Data: []byte("x")})
			if errors.Is(err, pubqueue.ErrQueueFull) {
				break
			}
			if err != nil {
				t.Fatalf("Push: %v", err)
			}
			filled++
		}

		// Overflow must be reported, not dropped.
		for i := 0; i < 10; i++ {
			if err := p.q.Push(pubqueue.Item{Subject: subject}); !errors.Is(err, pubqueue.ErrQueueFull) {
				t.Fatalf("overflow Push %d: got %v, want ErrQueueFull", i, err)
			}
		}

		p.close()
		if err := nc.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		// Nothing panicked and every overflow was surfaced.
	})
}

// TestPubQueueBlockingLosesNothing drives a blocking publisher past its
// capacity and asserts every enqueued message reaches the server.
func TestPubQueueBlockingLosesNothing(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		const subject = "pq.block"
		const total = 10000

		sub, totalReceived := countReceiver(t, nc, subject)
		defer sub.Unsubscribe()

		p := newAsyncPublisher(t, nc, subject, 16)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for i := 0; i < total; i++ {
			err := p.q.PushWait(ctx, pubqueue.Item{
				Subject: subject,
				Data:    []byte(fmt.Sprintf("msg-%d", i)),
			})
			if err != nil {
				t.Fatalf("PushWait(%d): %v", i, err)
			}
		}

		p.close()
		if err := nc.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		// Give the receiver a moment to drain the last buffered messages.
		waitForCount(t, totalReceived, total, 2*time.Second)
	})
}

// TestPubQueueTenThousandCapacity pins the documented capacity and ensures a
// ten-thousand-slot queue accepts exactly that many items before reporting
// full, then delivers all of them.
func TestPubQueueTenThousandCapacity(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		const subject = "pq.cap"
		const capacity = pubqueue.DefaultPendingCapacity
		if capacity != 10000 {
			t.Fatalf("DefaultPendingCapacity = %d, want 10000", capacity)
		}

		sub, totalReceived := countReceiver(t, nc, subject)
		defer sub.Unsubscribe()

		p := newAsyncPublisher(t, nc, subject, capacity)
		if p.q.Cap() != capacity {
			t.Fatalf("Cap = %d, want %d", p.q.Cap(), capacity)
		}

		// Pause draining by filling faster than the writer under no server
		// pressure; instead assert the queue reports full at the boundary.
		// Use a standalone queue here so the writer goroutine cannot drain.
		full, err := pubqueue.New(capacity)
		if err != nil {
			t.Fatalf("pubqueue.New: %v", err)
		}
		for i := 0; i < capacity; i++ {
			if err := full.Push(pubqueue.Item{}); err != nil {
				t.Fatalf("Push(%d): %v", i, err)
			}
		}
		if err := full.Push(pubqueue.Item{}); !errors.Is(err, pubqueue.ErrQueueFull) {
			t.Fatalf("boundary Push: got %v, want ErrQueueFull", err)
		}
		full.Close()

		// Now publish capacity messages through the live async publisher and
		// confirm every one arrives.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for i := 0; i < capacity; i++ {
			if err := p.q.PushWait(ctx, pubqueue.Item{Subject: subject}); err != nil {
				t.Fatalf("PushWait(%d): %v", i, err)
			}
		}
		p.close()
		if err := nc.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		waitForCount(t, totalReceived, capacity, 5*time.Second)
	})
}

// TestPubQueueCloseDrainsBuffered ensures buffered messages are not lost when
// the publisher closes after a burst.
func TestPubQueueCloseDrainsBuffered(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		const subject = "pq.drain"
		const total = 500

		sub, totalReceived := countReceiver(t, nc, subject)
		defer sub.Unsubscribe()

		p := newAsyncPublisher(t, nc, subject, total)
		for i := 0; i < total; i++ {
			if err := p.q.Push(pubqueue.Item{Subject: subject}); err != nil {
				t.Fatalf("Push(%d): %v", i, err)
			}
		}

		// Close must still deliver the fully-buffered queue.
		p.close()
		if err := nc.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		waitForCount(t, totalReceived, total, 2*time.Second)
	})
}

func waitForCount(t *testing.T, current func() int, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if current() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := current(); got != want {
		t.Fatalf("delivered = %d, want %d (lost %d)", got, want, want-got)
	}
}
