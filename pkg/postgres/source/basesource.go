package source

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/postgres/common/cursor"
	"net"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	INITBEGIN = 1
	INITEND   = 2
	STOP      = 3
)

type CaptureFn func(changes chan Change) error
type FlushFn func()
type ReadFn func(ctx context.Context) (Change, error)

type Change struct {
	Checkpoint cursor.Checkpoint
	Message    *pb.Message
}

type Source interface {
	Capture(cp cursor.Checkpoint) (changes chan Change, err error)
	Commit(cp cursor.Checkpoint)
	Error() error
	Stop() error
}

type BaseSource struct {
	ReadTimeout time.Duration

	state   int64
	stopped chan struct{}

	err atomic.Value
}

func (b *BaseSource) Capture(cp cursor.Checkpoint) (changes chan Change, err error) {
	log.Errorf("%v", cp)
	panic("implement me")
}

func (b *BaseSource) Commit(cp cursor.Checkpoint) {
	log.Errorf("%v", cp)
	panic("implement me")
}

func (b *BaseSource) Stop() error {
	switch atomic.LoadInt64(&b.state) {
	case INITBEGIN, INITEND:
		for !atomic.CompareAndSwapInt64(&b.state, INITEND, STOP) {
			runtime.Gosched()
		}
		fallthrough
	case STOP:
		<-b.stopped
	}
	return b.Error()
}

func (b *BaseSource) Error() error {
	if err, ok := b.err.Load().(error); ok {
		return err
	}
	return nil
}

func (b *BaseSource) capture(readFn ReadFn, flushFn FlushFn) (chan Change, error) {
	if !atomic.CompareAndSwapInt64(&b.state, 0, INITBEGIN) {
		return nil, nil
	}

	b.stopped = make(chan struct{})
	changes := make(chan Change, 1000)

	atomic.StoreInt64(&b.state, INITEND)

	timeout := b.ReadTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	go func() {
		defer close(b.stopped)
		defer close(changes)
		defer flushFn()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			change, err := readFn(ctx)
			cancel()
			if atomic.LoadInt64(&b.state) != INITEND {
				return
			}
			if isTimeout(err) {
				continue
			}
			if err != nil {
				b.err.Store(fmt.Errorf("%w", err))
				return
			}
			if change.Message != nil {
				changes <- change
			}
		}
	}()

	return changes, nil
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
