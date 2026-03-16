package netplus_test

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/likexian/gokit/assert"
	"github.com/helios-live/go-netplus/v2"
)

func TestCappingReads(t *testing.T) {
	running := true
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	nn := 0
	go func() {
		for running {
			c, err := ln.Accept()

			cc := netplus.CounterConn{Conn: c, Cap: 15000}
			if err != nil {
				panic(err)
			}
			b := make([]byte, 100)
			err = nil
			n := 0
			for running && err == nil {
				n, err = cc.Read(b)
				if err == nil {
					nn += n
				}
			}
			running = false
			assert.Equal(t, err, io.EOF)
		}
	}()
	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}
		cc := netplus.CounterConn{Conn: c}
		buf := []byte("omg it works!")
		for running && err == nil {
			_, err = cc.Write(buf)
		}
		running = false
	}()
	for running {
		time.Sleep(time.Millisecond)
	}
	assert.Ge(t, nn, 14000, fmt.Sprintf("Expected nn to be > %d, value: %d", 14000, nn))
	assert.Le(t, nn, 15000, fmt.Sprintf("Expected nn to be < %d, value: %d", 15000, nn))
}

func TestCappingWrites(t *testing.T) {
	running := true
	nn := 0
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		for running {
			c, err := ln.Accept()
			if err != nil {
				panic(err)
			}
			cc := netplus.CounterConn{Conn: c}
			b := make([]byte, 100)
			err = nil
			for running && err == nil {
				_, err = cc.Read(b)
			}
			running = false
		}
	}()
	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}
		cc := netplus.CounterConn{Conn: c, Cap: 15000}
		buf := []byte("omg it works!")
		n := 0
		for running && err == nil {
			n, err = cc.Write(buf)
			if err == nil {
				nn += n
			}
		}
		running = false
		assert.Equal(t, err, io.EOF)
	}()
	for running {
		time.Sleep(time.Millisecond)
	}
	assert.Ge(t, nn, 14000, fmt.Sprintf("Expected nn to be > %d, value: %d", 14000, nn))
	assert.Le(t, nn, 15000, fmt.Sprintf("Expected nn to be < %d, value: %d", 15000, nn))
}

func TestClosingReads(t *testing.T) {
	running := true
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	nn := 0
	go func() {
		for running {
			c, err := ln.Accept()

			cc := netplus.CounterConn{Conn: c, Cap: 15000}
			if err != nil {
				panic(err)
			}
			b := make([]byte, 100)
			err = nil
			n := 0
			for running && err == nil {
				n, err = cc.Read(b)
				if err == nil {
					nn += n
				}
			}
			assert.Equal(t, err, io.EOF)
		}
	}()
	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}
		cc := netplus.CounterConn{Conn: c}
		buf := []byte("omg it works!")
		for err == nil {
			_, err = cc.Write(buf)
		}
		running = false
		assert.NotNil(t, err)
	}()
	for running {
		time.Sleep(time.Millisecond)
	}
	assert.Ge(t, nn, 14000, fmt.Sprintf("Expected nn to be > %d, value: %d", 14000, nn))
	assert.Le(t, nn, 15000, fmt.Sprintf("Expected nn to be < %d, value: %d", 15000, nn))
}
