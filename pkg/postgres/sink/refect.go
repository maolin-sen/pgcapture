package sink

import (
	"errors"
	"fmt"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/postgres/common/cursor"
	"reflect"
	"strings"
)

type Model interface {
	TableName() (schema, table string)
}

type ConsumerChange struct {
	Op         pb.Change_Operation
	Checkpoint cursor.Checkpoint
	New        interface{}
	Old        interface{}
}

type ModelHandlerFunc func(change ConsumerChange) error
type ModelAsyncHandlerFunc func(change ConsumerChange, done func(err error))
type ModelHandlers map[Model]ModelHandlerFunc
type ModelAsyncHandlers map[Model]ModelAsyncHandlerFunc //map[tableName]ModelAsyncHandlerFunc

func ToAsyncHandlerFunc(fn ModelHandlerFunc) ModelAsyncHandlerFunc {
	return func(change ConsumerChange, done func(err error)) {
		done(fn(change))
	}
}

// ReflectModel 填充reflection的idx，typ字段
func ReflectModel(model Model) (ref Reflection, err error) {
	typ := reflect.TypeOf(model)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return ref, errors.New("the field Model of SwitchHandler should be a pointer of struct")
	}
	typ = typ.Elem()
	ref = Reflection{Idx: make(map[string]int, typ.NumField()), Typ: typ}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if tag, ok := f.Tag.Lookup("pg"); ok {
			if n := strings.Split(tag, ","); len(n) > 0 && n[0] != "" {
				ref.Idx[n[0]] = i
			}
		}
	}
	for k := range ref.Idx {
		if k != "" {
			return ref, nil
		}
	}
	return ref, fmt.Errorf("at least one field of %s should should have a valid pg tag", typ.Elem())
}

func ModelName(namespace, table string) string {
	if namespace == "" {
		return "public." + table
	}
	return namespace + "." + table
}

type Reflection struct {
	Idx map[string]int
	Typ reflect.Type
	Hdl ModelAsyncHandlerFunc
}
