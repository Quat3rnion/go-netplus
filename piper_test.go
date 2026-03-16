package netplus_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/likexian/gokit/assert"
	"github.com/helios-live/go-log/v2"
	"github.com/helios-live/go-netplus/v2"
)

var loglevel = 1

type nopwriter struct {
	*io.PipeReader
}

func (c *nopwriter) Write(p []byte) (n int, err error) {
	fmt.Fprintln(os.Stderr, "writing", len(p))
	return len(p), nil
}

type nopreader struct {
	*io.PipeWriter
	closed bool
}

func (c *nopreader) Close() error {
	c.PipeWriter.Close()
	c.closed = true
	time.Sleep(10 * time.Second)
	fmt.Fprintln(os.Stderr, "close called")
	return nil
}

func (c *nopreader) Read(p []byte) (n int, err error) {
	if c.closed {
		return 0, io.EOF
	}
	if loglevel > 9999 {
		fmt.Fprint(os.Stderr, ". ")
	}
	time.Sleep(1 * time.Millisecond)
	return 0, nil
}

var (
	running = true
	m       = sync.Mutex{}
)

func isRunning() bool {
	m.Lock()
	defer m.Unlock()
	return running
}

func stopRunning() {
	m.Lock()
	defer m.Unlock()
	running = false
}

func TestWriteTimeoutReached(t *testing.T) {
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
			// }
			assert.Nil(t, err)
		}
	}()

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}

		piper := netplus.NewPiper(logger, 2*time.Second)
		piper.Debug(true)

		// context with timeout 2 seconds
		ctx := context.Background()

		reader, writer := io.Pipe()
		go func() {
			// write 1 byte every 10 milliseconds
			b := make([]byte, 32000)
			for i := 0; i < 100000 && isRunning(); i++ {
				time.Sleep(10 * time.Millisecond)
				writer.Write(b)
			}
		}()

		_, err = piper.Run(ctx, c, &nopwriter{reader})
		stopRunning()
		logger.Debug("piper.Run", err)
		assert.NotNil(t, err)
	}()

	for isRunning() {
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkBufferAllocation(b *testing.B) {
	sizes := []int{32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			b.Run("Make", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					buf := make([]byte, size)
					_ = buf
				}
			})

			pool := sync.Pool{
				New: func() interface{} {
					return make([]byte, size)
				},
			}

			b.Run("SyncPool", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					buf := pool.Get().([]byte)
					_ = buf
					pool.Put(buf)
				}
			})
		})
	}
}

func TestReadTimeoutReached(t *testing.T) {
	running = true
	logger := log.NewZero(os.Stderr)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	nn := 0
	go func() {
		for isRunning() {
			c, err := ln.Accept()

			b := make([]byte, 100)
			err = nil
			n := 0

			reader, writer := io.Pipe()

			piper := netplus.NewPiper(logger, 2*time.Second)
			piper.Debug(true)

			go func() {
				for isRunning() && err == nil {
					n, err = reader.Read(b)
					if err == nil {
						nn += n
					}
				}
			}()

			_, err = piper.Run(context.Background(), c, &nopreader{writer, false})
			stopRunning()
			logger.Debug("piper.Run", err)
			assert.Nil(t, err)
		}
	}()

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			panic(err)
		}

		b := make([]byte, 32)
		_, err = c.Write(b)

		assert.Nil(t, err)
	}()

	for isRunning() {
		time.Sleep(time.Millisecond)
	}
}
