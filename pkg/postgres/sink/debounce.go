package sink

import (
	"context"
	"fmt"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/postgres/common/cursor"
	"github.com/replicase/pgcapture/pkg/postgres/source"
	"sync"
	"time"
)

func debounceKey(m interface{}) string {
	model := m.(DebounceModel)
	schema, table := model.TableName()
	return schema + table + model.DebounceKey()
}

type BounceHandler interface {
	Initialize(ctx context.Context, mh ModelAsyncHandlers) error
	Handle(fn ModelAsyncHandlerFunc, checkpoint cursor.Checkpoint, change ConsumerChange)
}

type DebounceModel interface {
	Model
	DebounceKey() string
}

type event struct {
	Checkpoint cursor.Checkpoint
	Change     ConsumerChange
	Handler    ModelAsyncHandlerFunc
}

// DebounceHandler 防抖动
type DebounceHandler struct {
	Interval time.Duration
	source   source.Source
	store    map[string]event
	ctx      context.Context
	mu       sync.Mutex
}

// Initialize 初始化事件处理
func (b *DebounceHandler) Initialize(ctx context.Context, mh ModelAsyncHandlers) error {
	//parameter check
	for model := range mh {
		if _, ok := model.(DebounceModel); !ok {
			schema, table := model.TableName()
			return fmt.Errorf("%s.%s model should be implemented with DebounceModel interface", schema, table)
		}
	}
	b.ctx = ctx
	b.store = make(map[string]event)

	go func() {
		var err error
		//定时处理事件
		for err == nil {
			time.Sleep(b.Interval)

			//出错，退出循环
			select {
			case <-b.ctx.Done():
				err = b.ctx.Err()
			default:
			}

			b.mu.Lock()
			for k, v := range b.store {
				b.handle(v)
				delete(b.store, k)
			}
			b.mu.Unlock()
		}
	}()

	return nil
}

func (b *DebounceHandler) Handle(fn ModelAsyncHandlerFunc, checkpoint cursor.Checkpoint, change ConsumerChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := event{
		Checkpoint: checkpoint,
		Change:     change,
		Handler:    fn,
	}

	switch change.Op {
	case pb.Change_INSERT:
		if prev, ok := b.store[debounceKey(change.New)]; ok {
			b.handle(prev)
		}
		b.handle(e)
	case pb.Change_DELETE:
		if prev, ok := b.store[debounceKey(change.Old)]; ok {
			b.handle(prev)
		}
		b.handle(e)
	case pb.Change_UPDATE:
		if change.Old != nil {
			if prev, ok := b.store[debounceKey(change.Old)]; ok {
				b.handle(prev)
			}
		}
		key := debounceKey(change.New)
		if prev, ok := b.store[key]; ok {
			b.source.Commit(prev.Checkpoint)
		}
		b.store[key] = e //缓存update的数据
	}
}

// Handler <=> fn:用户自定义处理函数
func (b *DebounceHandler) handle(e event) {
	e.Handler(e.Change, func(err error) {
		if err != nil {
			//b.source.Requeue(e.Checkpoint, err.Error())
		} else {
			b.source.Commit(e.Checkpoint)
		}
	})
}

/**********************************************************************************************************************/

type NoBounceHandler struct {
	source source.Source
}

func (b *NoBounceHandler) Initialize(ctx context.Context, mh ModelAsyncHandlers) error {
	return nil
}

func (b *NoBounceHandler) Handle(fn ModelAsyncHandlerFunc, checkpoint cursor.Checkpoint, change ConsumerChange) {
	fn(change, func(err error) {
		if err != nil {
			//b.source.Requeue(checkpoint, err.Error())
		} else {
			b.source.Commit(checkpoint)
		}
	})
}
