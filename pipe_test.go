package netplus_test

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/likexian/gokit/assert"
	"github.com/helios-live/go-log/v2"
	"github.com/helios-live/go-netplus/v2"
)

func TestPipeReadTimeoutReached(t *testing.T) {
	logger := log.NewZero(os.Stderr)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	nn := 0
	go func() {
		for isRunning() {
			c, _ := ln.Accept()

			b := make([]byte, 100)
			err = nil
			n := 0
			// for running && err == nil {
			n, err = c.Read(b)
			logger.Debug("read", n)
			if err == nil {
				nn += n
			}
			isEof := err == io.EOF
			assert.True(t, isEof)
		}
	}()

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}
		d, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}

		pipe := netplus.NewPipe(logger, 2*time.Second)

		// context with timeout 2 seconds
		ctx := context.Background()

		// go func() {
		// 	// write 1 byte every 10 milliseconds
		// 	b := make([]byte, 32000)
		// 	for i := 0; i < 100000 && isRunning(); i++ {
		// 		time.Sleep(10 * time.Millisecond)
		// 		n, _ := c.Write(b)
		// 		logger.Debug("write", n)
		// 	}
		// }()

		err = pipe.Run(ctx, c, d)
		stopRunning()
		logger.Debug("piper.Run", err)
		netErr := err.(net.Error)
		assert.True(t, netErr.Timeout())
	}()

	for isRunning() {
		time.Sleep(time.Millisecond)
	}
}

func TestPipeWriteTimeoutReached(t *testing.T) {
	running = true
	logger := log.NewZero(os.Stderr)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	// nn := 0
	go func() {
		c, err := ln.Accept()
		d, err := ln.Accept()

		b := make([]byte, 100)
		err = nil
		n := 0
		nn := 0

		// reader, _ := io.Pipe()

		pipe := netplus.NewPipe(logger, 4*time.Second)

		go func() {
			for isRunning() && err == nil {
				n, err = c.Write(b)
				if err == nil {
					nn += n
				}
			}
		}()

		err = pipe.Run(context.Background(), c, d)
		stopRunning()
		logger.Debug("piper.Run", err)
		netErr := err.(net.Error)
		assert.True(t, netErr.Timeout())
	}()

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}
		d, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}

		b := make([]byte, 32000)
		for i := 0; i < 100000 && isRunning(); i++ {
			_, err = c.Write(b)
			_, err = d.Write(b)
			time.Sleep(10 * time.Millisecond)
		}

		assert.Nil(t, err)
	}()

	for isRunning() {
		time.Sleep(time.Millisecond)
	}
}
