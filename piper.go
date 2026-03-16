package netplus

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/helios-live/go-log/v2"
)

// ErrShortWrite means that a write accepted fewer bytes than requested
// but failed to return an explicit error.
var ErrShortWrite = errors.New("short write")

// errInvalidWrite means that a write returned an impossible count.
var errInvalidWrite = errors.New("invalid write result")

// Piper .
type Piper struct {
	Logger     log.Logger
	Timeout    time.Duration
	debugLevel int
}

var pool sync.Pool

func init() {
	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024)
		},
	}
}

// NewPiper returns a pointer to a newPiper Piper instance
func NewPiper(l log.Logger, t time.Duration) *Piper {
	return &Piper{
		Logger:  l,
		Timeout: t,
	}
}

// Debug turns debugging on and off
func (p *Piper) Debug(debug bool) {
	p.debugLevel = 1
}

// DebugLevel turns debugging level to a specific level
func (p *Piper) DebugLevel(debug int) {
	p.debugLevel = debug
}

// Run pipes data between upstream and downstream and closes one when the other closes
// times out after two hours by default
func (p *Piper) Run(ctx context.Context, downstream io.ReadWriteCloser, upstream io.ReadWriteCloser) (written int64, err error) {
	var dur time.Duration
	if p.Timeout == 0 {
		dur = time.Duration(2 * time.Hour)
	} else {
		dur = p.Timeout
	}
	return p.idleTimeoutPipe(ctx, downstream, upstream, dur)
}

func (p *Piper) idleTimeoutPipe(ctx context.Context, dst io.ReadWriteCloser, src io.ReadWriteCloser, timeout time.Duration) (written int64, err error) {
	if p.debugLevel > 9999 {
		p.Logger.Debug("runnning idleTimeoutPipe for ", timeout)
	}
	var running int32 = 1

	ctx, closeContext := context.WithCancel(ctx)

	upstreamReset := make(chan struct{})
	downstreammReset := make(chan struct{})
	closeBothSockets := func(from string) {
		if p.debugLevel > 9999 {
			p.Logger.Debug("closeBothSockets called from ", from)
		}

		if !atomic.CompareAndSwapInt32(&running, 1, 0) {
			return
		}
		if p.debugLevel > 9999 {
			p.Logger.Debug("Swapped")
		}
		closeContext()
		src.Close()
		dst.Close()
		if p.debugLevel > 9999 {
			p.Logger.Debug("closing")
		}
		ctx.Done()
	}
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop() // Stop the timer when the goroutine exits

		for {
			select {
			case <-ctx.Done():
				closeBothSockets("ctx.Done")
				return
			case <-timer.C:
				if p.debugLevel > 0 {
					p.Logger.Debug("idletimeoutpipe: timeout reached")
				}
				closeBothSockets("idle")
				return
			case <-upstreamReset:
				timer.Reset(timeout)
			case <-downstreammReset:
				timer.Reset(timeout)
			}
		}
	}()
	var w1, w2 int64
	var err1, err2 error
	ec := make(chan error, 2)
	go func() {
		w1, err1 = copy(src, dst, upstreamReset)
		ec <- err1
	}()
	go func() {
		w2, err2 = copy(dst, src, downstreammReset)
		ec <- err2
	}()
	firstErr := <-ec
	closeBothSockets("end of Run")
	if p.debugLevel > 9999 {
		p.Logger.Debug("Emptying channel")
	}
	// give the other goroutine a chance to finish ( 1 second ) before just ignoring that goroutine
	select {
	case <-ec: // empty the channel, equivallent to wg.Wait
	case <-time.After(1 * time.Second):
	}
	if p.debugLevel > 9999 {
		p.Logger.Debug("Emptied channel")
	}

	return w1 + w2, firstErr
}

func copy(src io.Reader, dst io.Writer, timekeeper chan struct{}) (written int64, err error) {
	defer close(timekeeper)

	// buf := make([]byte, size)
	buf := pool.Get().([]byte)
	defer pool.Put(buf)

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errInvalidWrite
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = ErrShortWrite
				break
			}
			// non blocking send
			select {
			case timekeeper <- struct{}{}:
			default:
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
