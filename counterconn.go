package netplus

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const MaxMinutes = 10

// CounterConn counts all bytes that go through it.
//
// Upstream/Downstream/Cap are atomic.Int64 so that observers (e.g. metrics or
// accountant goroutines) can safely read them concurrently with the Read/Write
// goroutines that mutate them.
type CounterConn struct {
	net.Conn
	Upstream   atomic.Int64
	Downstream atomic.Int64
	Cap        atomic.Int64
}

// CounterListener is the Listener that uses CounterConn instead of net.Conn
type CounterListener struct {
	net.Listener
	rpm        *[MaxMinutes]int64
	LastMinute *int64
	mux        *sync.Mutex
}

func NewCounterListener(l net.Listener) *CounterListener {
	return &CounterListener{
		Listener:   l,
		rpm:        &[MaxMinutes]int64{},
		LastMinute: new(int64),
		mux:        &sync.Mutex{},
	}
}

// Accept wraps the inner net.Listener accept and returns a CounterConn
func (cl CounterListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	minuteEpoch := time.Now().Unix() / 60
	cl.mux.Lock()

	if minuteEpoch != *cl.LastMinute {

		for i := MaxMinutes - 1; i > 0; i-- {
			cl.rpm[i] = cl.rpm[i-1]
		}
		cl.rpm[0] = 1
		*cl.LastMinute = minuteEpoch
	} else {
		cl.rpm[0]++
	}
	cl.mux.Unlock()

	return &CounterConn{Conn: conn}, err
}

func (cl *CounterListener) GetRPM() [MaxMinutes]int64 {
	cl.mux.Lock()
	defer cl.mux.Unlock()
	return *cl.rpm
}

func (cc *CounterConn) Read(b []byte) (int, error) {
	n, err := cc.Conn.Read(b)
	n6 := int64(n)
	cc.Upstream.Add(n6)

	// what is the null value?
	// one option is to use zero as a null value

	cap := cc.Cap.Load()

	if cap == 0 {
		return n, err
	}

	cc.Cap.Add(-n6)

	nv := cap - n6

	if nv > 0 {
		return n, err
	}
	if nv < 0 {
		cc.Conn.Close()
		return n, io.EOF
	}
	// we use the zero value as a way to tell that there is no cap set
	cc.Cap.Add(-1)
	return n, err
}

func (cc *CounterConn) Write(b []byte) (int, error) {
	n, err := cc.Conn.Write(b)
	n6 := int64(n)
	cc.Downstream.Add(n6)

	cap := cc.Cap.Load()

	if cap == 0 {
		return n, err
	}
	cc.Cap.Add(-n6)

	nv := cap - n6

	if nv > 0 {
		return n, err
	}
	if nv < 0 {
		cc.Conn.Close()
		return n, io.EOF
	}
	// we use the zero value as a way to tell that there is no cap set
	cc.Cap.Add(-1)
	return n, err
}
