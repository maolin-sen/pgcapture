package sink

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	pgtypeV4 "github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/postgres/common/cursor"
	"github.com/replicase/pgcapture/pkg/postgres/source"
	"reflect"
	"time"
)

var (
	ci      = pgtypeV4.NewConnInfo()
	typeMap = pgtype.NewMap()
)
var DefaultErrorFn = func(source source.Change, err error) {}

type OnDecodeError func(source source.Change, err error)
type Consumer interface {
	ConsumeAsync(mah ModelAsyncHandlers) error
	Consume(mh ModelHandlers) error
	Stop()
}
type ConsumerOption struct {
	URI              string
	TableRegex       string
	DebounceInterval time.Duration
	OnDecodeError    OnDecodeError
}
type defaultConsumer struct {
	Source  source.Source
	Bouncer BounceHandler
	ctx     context.Context
	errFn   OnDecodeError
}

func NewConsumer(ctx context.Context, src source.Source, option ConsumerOption) Consumer {
	errFn := DefaultErrorFn
	if option.OnDecodeError != nil {
		errFn = option.OnDecodeError
	}

	consumer := &defaultConsumer{ctx: ctx, Source: src, errFn: errFn}
	if option.DebounceInterval > 0 {
		consumer.Bouncer = &DebounceHandler{
			Interval: option.DebounceInterval,
			source:   src,
		}
	} else {
		consumer.Bouncer = &NoBounceHandler{source: src}
	}
	return consumer
}

func (c *defaultConsumer) ConsumeAsync(mah ModelAsyncHandlers) error {
	if err := c.Bouncer.Initialize(c.ctx, mah); err != nil {
		return err
	}

	//map[表名]reflection
	refs := make(map[string]Reflection, len(mah))
	for m, h := range mah {
		ref, err := ReflectModel(m)
		if err != nil {
			return err
		}
		ref.Hdl = h
		refs[ModelName(m.TableName())] = ref
	}

	// 内部起一个协程接收change到一个缓存chan
	changes, err := c.Source.Capture(cursor.Checkpoint{})
	if err != nil {
		return err
	}

	for change := range changes {
		switch m := change.Message.Type.(type) {
		case *pb.Message_Change:
			ref, ok := refs[ModelName(m.Change.Schema, m.Change.Table)]
			if !ok {
				break
			}
			newRecord, err := makeModel(ref, m.Change.New)
			if err != nil {
				c.errFn(change, err)
				break
			}
			oldRecord, err := makeModel(ref, m.Change.Old)
			if err != nil {
				c.errFn(change, err)
				break
			}
			c.Bouncer.Handle(ref.Hdl, change.Checkpoint, ConsumerChange{
				Op:         m.Change.Op,
				Checkpoint: change.Checkpoint,
				New:        newRecord,
				Old:        oldRecord,
			})
			continue
		}
		c.Source.Commit(change.Checkpoint)
	}
	return c.Source.Error()
}

func (c *defaultConsumer) Consume(mh ModelHandlers) error {
	mah := make(ModelAsyncHandlers, len(mh))
	for m, fn := range mh {
		mah[m] = ToAsyncHandlerFunc(fn)
	}
	return c.ConsumeAsync(mah)
}

func (c *defaultConsumer) Stop() {
	err := c.Source.Stop()
	if err != nil {
		log.Errorf("stop failed,error:%v", err)
		return
	}
}

func makeModel(ref Reflection, fields []*pb.Field) (interface{}, error) {
	ptr := reflect.New(ref.Typ)
	val := ptr.Elem()
	interfaces := make(map[string]interface{}, len(ref.Idx))
	for name, i := range ref.Idx {
		if f := val.Field(i).Addr(); f.CanInterface() {
			interfaces[name] = f.Interface()
		}
	}
	var err error
	for _, f := range fields {
		field, ok := interfaces[f.Name]
		if !ok {
			continue
		}
		if f.Value == nil {
			if decoder, ok := field.(pgtypeV4.BinaryDecoder); ok {
				err = decoder.DecodeBinary(ci, nil)
			} else {
				err = typeMap.Scan(f.Oid, pgtype.BinaryFormatCode, nil, field)
			}
		} else {
			if value, ok := f.Value.(*pb.Field_Binary); ok {
				if decoder, ok := field.(pgtypeV4.BinaryDecoder); ok {
					err = decoder.DecodeBinary(ci, value.Binary)
				} else {
					err = typeMap.Scan(f.Oid, pgtype.BinaryFormatCode, f.GetBinary(), field)
				}
			} else {
				if decoder, ok := field.(pgtypeV4.TextDecoder); ok {
					err = decoder.DecodeText(ci, []byte(f.GetText()))
				} else {
					err = typeMap.Scan(f.Oid, pgtype.TextFormatCode, []byte(f.GetText()), field)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return ptr.Interface(), nil
}
