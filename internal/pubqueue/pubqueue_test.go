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

package pubqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewRejectsBadCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		if _, err := New(capacity); err == nil {
			t.Fatalf("New(%d): expected error, got nil", capacity)
		}
	}
}

func TestPushReturnsFullInsteadOfDropping(t *testing.T) {
	const capacity = 4
	q, err := New(capacity)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := 0; i < capacity; i++ {
		if err := q.Push(Item{Subject: "foo", Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}
	if q.Len() != capacity {
		t.Fatalf("Len = %d, want %d", q.Len(), capacity)
	}

	// Every overflow attempt must be reported, never silently dropped.
	for i := 0; i < 3; i++ {
		if err := q.Push(Item{Subject: "overflow"}); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("Push on full queue: got %v, want ErrQueueFull", err)
		}
	}
	if q.Len() != capacity {
		t.Fatalf("full queue changed length: %d, want %d", q.Len(), capacity)
	}

	// A drained slot frees room again.
	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop: expected item")
	}
	if err := q.Push(Item{Subject: "after-pop"}); err != nil {
		t.Fatalf("Push after Pop: %v", err)
	}
}

func TestPopFIFOOrder(t *testing.T) {
	q, _ := New(5)
	for i := 0; i < 5; i++ {
		if err := q.Push(Item{Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		item, ok := q.Pop()
		if !ok {
			t.Fatalf("Pop(%d): closed unexpectedly", i)
		}
		if item.Data[0] != byte(i) {
			t.Fatalf("Pop(%d) = %d, want %d", i, item.Data[0], i)
		}
	}
}

func TestPushWaitBlocksUntilSlot(t *testing.T) {
	q, _ := New(1)
	if err := q.Push(Item{Subject: "first"}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	unblocked := make(chan error, 1)
	go func() {
		unblocked <- q.PushWait(context.Background(), Item{Subject: "second"})
	}()

	// Give the waiter time to park.
	select {
	case err := <-unblocked:
		t.Fatalf("PushWait returned before a slot was free: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop: expected item")
	}

	select {
	case err := <-unblocked:
		if err != nil {
			t.Fatalf("PushWait after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("PushWait did not unblock after a slot opened")
	}

	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1", q.Len())
	}
}

func TestPushWaitContextTimeout(t *testing.T) {
	q, _ := New(1)
	if err := q.Push(Item{}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := q.PushWait(ctx, Item{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PushWait: got %v, want DeadlineExceeded", err)
	}
	if time.Since(start) < 15*time.Millisecond {
		t.Fatal("PushWait returned before the deadline elapsed")
	}
	// The timed-out item must not have been enqueued.
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1", q.Len())
	}
}

func TestPushWaitNilContext(t *testing.T) {
	q, _ := New(1)
	if err := q.PushWait(nil, Item{}); err == nil {
		t.Fatal("PushWait(nil): expected error")
	}
}

func TestCloseRejectsNewAndDrainsBuffered(t *testing.T) {
	q, _ := New(3)
	for i := 0; i < 3; i++ {
		if err := q.Push(Item{Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	q.Close()
	if !q.Closed() {
		t.Fatal("Closed() = false, want true")
	}

	if err := q.Push(Item{}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Push after Close: got %v, want ErrQueueClosed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.PushWait(ctx, Item{}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("PushWait after Close: got %v, want ErrQueueClosed", err)
	}

	// Buffered items must still be delivered in order.
	for i := 0; i < 3; i++ {
		item, ok := q.Pop()
		if !ok {
			t.Fatalf("Pop(%d): expected buffered item after Close", i)
		}
		if item.Data[0] != byte(i) {
			t.Fatalf("Pop(%d) = %d, want %d", i, item.Data[0], i)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on drained, closed queue: expected ok=false")
	}

	// Close is idempotent.
	q.Close()
}

func TestCloseUnblocksWaitingPush(t *testing.T) {
	q, _ := New(1)
	if err := q.Push(Item{}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- q.PushWait(context.Background(), Item{})
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case err := <-waitErr:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("blocked PushWait: got %v, want ErrQueueClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock PushWait")
	}
}

func TestConcurrentProducersNoLoss(t *testing.T) {
	const (
		capacity  = 64
		producers = 8
		perWorker = 200
	)
	q, _ := New(capacity)

	var wg sync.WaitGroup
	for worker := 0; worker < producers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := q.PushWait(context.Background(), Item{})
				if err != nil {
					t.Errorf("PushWait: %v", err)
					return
				}
			}
		}()
	}

	received := 0
	done := make(chan struct{})
	go func() {
		for {
			_, ok := q.Pop()
			if !ok {
				close(done)
				return
			}
			received++
		}
	}()

	wg.Wait()
	q.Close()
	<-done

	if want := producers * perWorker; received != want {
		t.Fatalf("received = %d, want %d (lost %d)", received, want, want-received)
	}
}

func TestConcurrentPushAndCloseNoPanic(t *testing.T) {
	q, _ := New(32)
	var wg sync.WaitGroup

	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for i := 0; i < 500; i++ {
				if err := q.PushWait(ctx, Item{}); err != nil {
					// Only timeouts and closed are acceptable once Close wins.
					if !errors.Is(err, context.DeadlineExceeded) &&
						!errors.Is(err, ErrQueueClosed) &&
						!errors.Is(err, context.Canceled) {
						t.Errorf("unexpected PushWait error: %v", err)
					}
					return
				}
			}
		}()
	}

	// A consumer keeps the queue moving while Close races the producers.
	go func() {
		for range q.items {
		}
	}()

	time.Sleep(10 * time.Millisecond)
	q.Close()
	wg.Wait()

	// Producers must observe the closed state, not a panic.
	if !q.Closed() {
		t.Fatal("queue not marked closed")
	}
}

func TestPushFastPathNoAllocation(t *testing.T) {
	q, _ := New(8)
	item := Item{Subject: "foo", Data: []byte("payload")}

	allocs := testing.AllocsPerRun(100, func() {
		if err := q.Push(item); err != nil {
			t.Fatalf("Push: %v", err)
		}
		// Drain to keep the queue from filling across iterations.
		if _, ok := q.Pop(); !ok {
			t.Fatal("Pop: expected item")
		}
	})

	if allocs != 0 {
		t.Fatalf("Push fast path allocated %v times per run, want 0", allocs)
	}
}
