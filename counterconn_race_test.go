package netplus_test

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/helios-live/go-netplus/v2"
)

// TestCounterConn_ConcurrentReadersDoNotRace exercises the counter fields
// from three goroutines simultaneously: one driving Write (mutates Downstream),
// one driving Read (mutates Upstream), and one polling Load() on both. With
// non-atomic fields, the third goroutine's Load equivalents would race with
// the += mutations performed by Read/Write. The race detector must stay quiet.
func TestCounterConn_ConcurrentReadersDoNotRace(t *testing.T) {
	a, b := net.Pipe()
	writer := &netplus.CounterConn{Conn: a}
	reader := &netplus.CounterConn{Conn: b}

	const runFor = 200 * time.Millisecond
	stop := make(chan struct{})

	var written, read int64
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		buf := []byte("hello-counterconn-race-buffer")
		for {
			select {
			case <-stop:
				return
			default:
			}
			n, err := writer.Write(buf)
			atomic.AddInt64(&written, int64(n))
			if err != nil {
				if !isClosedPipeErr(err) {
					t.Errorf("write: %v", err)
				}
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		for {
			n, err := reader.Read(buf)
			atomic.AddInt64(&read, int64(n))
			if err != nil {
				if !errors.Is(err, io.EOF) && !isClosedPipeErr(err) {
					t.Errorf("read: %v", err)
				}
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = writer.Downstream.Load()
			_ = writer.Upstream.Load()
			_ = reader.Downstream.Load()
			_ = reader.Upstream.Load()
		}
	}()

	time.Sleep(runFor)
	close(stop)
	_ = a.Close()
	_ = b.Close()
	wg.Wait()

	gotDownstream := writer.Downstream.Load()
	gotUpstream := reader.Upstream.Load()
	finalWritten := atomic.LoadInt64(&written)
	finalRead := atomic.LoadInt64(&read)

	if gotDownstream == 0 {
		t.Fatalf("writer.Downstream is 0; expected nonzero")
	}
	if gotUpstream == 0 {
		t.Fatalf("reader.Upstream is 0; expected nonzero")
	}
	if gotDownstream != finalWritten {
		t.Fatalf("writer.Downstream=%d, want %d (bytes returned by Write)", gotDownstream, finalWritten)
	}
	if gotUpstream != finalRead {
		t.Fatalf("reader.Upstream=%d, want %d (bytes returned by Read)", gotUpstream, finalRead)
	}
}

func isClosedPipeErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed)
}
