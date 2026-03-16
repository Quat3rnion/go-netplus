package netplus

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/helios-live/go-log/v2"
)

// Piper .
type Pipe struct {
	Logger     log.Logger
	Timeout    time.Duration
	debugLevel int
	running    int32
}

// NewPiper returns a pointer to a newPiper Piper instance
func NewPipe(l log.Logger, t time.Duration) *Pipe {
	return &Pipe{
		Logger:  l,
		Timeout: t,
	}
}

func (p *Pipe) Run(ctx context.Context, upstream net.Conn, downstream net.Conn) (err error) {
	if p.debugLevel > 9999 {
		p.Logger.Debug("runnning idleTimeoutPipe for ", p.Timeout)
	}
	p.running = 1

	ec := make(chan error, 2)
	go p.newcopy(upstream, downstream, ec)
	go p.newcopy(downstream, upstream, ec)

	var firstErr error

	// wait for the work to be done, either internally or externally
loop:
	for {
		select {
		case <-ctx.Done():

			break loop
		case firstErr = <-ec:

			break loop
		}
	}

	p.closeBothSockets(upstream, downstream)

	if p.debugLevel > 9999 {
		p.Logger.Debug("Emptying channel")
	}

	<-ec // drain the second value first
	close(ec)

	return firstErr
}

func (p *Pipe) newcopy(src net.Conn, dst net.Conn, chn chan error) {
	var err error

	buf := pool.Get().([]byte)
	defer pool.Put(buf)

	for {
		src.SetDeadline(time.Now().Add(p.Timeout))
		nr, er := src.Read(buf)

		if nr > 0 {

			dst.SetWriteDeadline(time.Now().Add(p.Timeout))
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errInvalidWrite
				}
			}

			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	chn <- err
}

func (p *Pipe) closeBothSockets(src net.Conn, dst net.Conn) {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return
	}

	if p.debugLevel > 9999 {
		p.Logger.Debug("Swapped")
	}

	// t := time.Now().Add(time.Millisecond * 100)
	// src.SetDeadline(t)
	// dst.SetDeadline(t)

	src.Close()
	dst.Close()

	if p.debugLevel > 9999 {
		p.Logger.Debug("closing")
	}
}
