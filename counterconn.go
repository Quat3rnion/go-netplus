package netplus

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const MaxMinutes = 10

// CounterConn counts all bytes that go through it
type CounterConn struct {
	net.Conn
	Upstream   int64
	Downstream int64
	Cap        int64
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

	return &CounterConn{conn, 0, 0, 0}, err
}

func (cl *CounterListener) GetRPM() [MaxMinutes]int64 {
	cl.mux.Lock()
	defer cl.mux.Unlock()
	return *cl.rpm
}

func (cc *CounterConn) Read(b []byte) (int, error) {
	n, err := cc.Conn.Read(b)
	n6 := int64(n)
	cc.Upstream += n6

	// what is the null value?
	// one option is to use zero as a null value

	cap := atomic.LoadInt64(&cc.Cap)

	if cap == 0 {
		return n, err
	}

	atomic.AddInt64(&cc.Cap, -n6)

	nv := cap - n6

	if nv > 0 {
		return n, err
	}
	if nv < 0 {
		cc.Conn.Close()
		return n, io.EOF
	}
	// we use the zero value as a way to tell that there is no cap set
	atomic.AddInt64(&cc.Cap, -1)
	return n, err
}

func (cc *CounterConn) Write(b []byte) (int, error) {
	n, err := cc.Conn.Write(b)
	n6 := int64(n)
	cc.Downstream += n6

	cap := atomic.LoadInt64(&cc.Cap)

	if cap == 0 {
		return n, err
	}
	atomic.AddInt64(&cc.Cap, -n6)

	nv := cap - n6

	if nv > 0 {
		return n, err
	}
	if nv < 0 {
		cc.Conn.Close()
		return n, io.EOF
	}
	// we use the zero value as a way to tell that there is no cap set
	atomic.AddInt64(&cc.Cap, -1)
	return n, err
}
