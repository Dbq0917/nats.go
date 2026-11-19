package test

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// injectReadDialer wraps the first connection and lets a test make the next
// Read return a complete protocol batch together with an error, the way a
// network read can when a connection fails.
type injectReadDialer struct {
	mu      sync.Mutex
	conn    *injectReadConn
	payload []byte
}

func (d *injectReadDialer) Dial(network, address string) (net.Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		return c, nil
	}
	d.conn = &injectReadConn{Conn: c, d: d}
	return d.conn, nil
}

func (d *injectReadDialer) inject(payload []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.payload = payload
	// Wake up a read that is already blocked in the connection.
	if d.conn != nil {
		d.conn.Conn.SetReadDeadline(time.Now())
	}
}

type injectReadConn struct {
	net.Conn
	d *injectReadDialer
}

func (c *injectReadConn) takePayload(p []byte) (int, error, bool) {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	if c.d.payload == nil {
		return 0, nil, false
	}
	n := copy(p, c.d.payload)
	c.d.payload = nil
	return n, io.EOF, true
}

func (c *injectReadConn) Read(p []byte) (int, error) {
	if n, err, ok := c.takePayload(p); ok {
		return n, err
	}
	n, err := c.Conn.Read(p)
	if nerr, eerr, ok := c.takePayload(p); ok {
		return nerr, eerr
	}
	return n, err
}

func TestReconnectDeliversMessagesReadBeforeDisconnect(t *testing.T) {
	for _, test := range []struct {
		name          string
		trailingFrame bool
	}{
		{"complete frames", true},
		{"complete payload without frame terminator", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			testReconnectDeliversMessagesReadBeforeDisconnect(t, test.trailingFrame)
		})
	}
}

func testReconnectDeliversMessagesReadBeforeDisconnect(t *testing.T, trailingFrame bool) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		d := &injectReadDialer{}
		var received atomic.Int32
		reconnected := make(chan struct{}, 1)
		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.SetCustomDialer(d),
			nats.ReconnectWait(20*time.Millisecond),
			nats.MaxReconnects(10),
			nats.ReconnectHandler(func(*nats.Conn) {
				select {
				case reconnected <- struct{}{}:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer nc.Close()
		if _, err := nc.Subscribe("foo", func(*nats.Msg) {
			received.Add(1)
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		const total = 20
		payload := make([]byte, 0, total*20)
		for i := 0; i < total; i++ {
			payload = append(payload, []byte("MSG foo 1 3\r\nbar")...)
			if trailingFrame || i != total-1 {
				payload = append(payload, '\r', '\n')
			}
		}
		d.inject(payload)

		select {
		case <-reconnected:
		case <-time.After(5 * time.Second):
			t.Fatalf("did not reconnect, status=%v received=%d", nc.Status(), received.Load())
		}
		deadline := time.Now().Add(2 * time.Second)
		for received.Load() != total && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if got := received.Load(); got != total {
			t.Fatalf("handler received %d messages, want %d", got, total)
		}
	})
}
