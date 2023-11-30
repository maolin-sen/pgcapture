package main

import (
	"context"
	"fmt"
	"github.com/replicase/pgcapture/pkg/postgres/common/decode"
	"github.com/replicase/pgcapture/pkg/postgres/sink"
	"github.com/replicase/pgcapture/pkg/postgres/source"
	"strconv"
	"time"
)

type Accounts struct {
	Id        int       `pg:"id"`
	UserId    string    `pg:"user_id"`
	UserName  string    `pg:"user_name"`
	GroupId   int       `pg:"group_id"`
	Source    int       `pg:"source"`
	Status    int       `pg:"status"`
	Utime     time.Time `pg:"utime"`
	Itime     time.Time `pg:"itime"`
	ExtraInfo string    `pg:"extra_info"`
	UserPwd   string    `pg:"user_pwd"`
	NgnSalt   string    `pg:"ngn_salt"`
}

func (a *Accounts) TableName() (schema, table string) {
	return "public", "accounts"
}

func (a *Accounts) DebounceKey() string {
	return strconv.Itoa(a.Id)
}

func main() {
	consumer := sink.NewConsumer(context.Background(), source.NewPGXSource(source.PGXSourceOption{
		SetupConnect: "postgresql://postgres:6uYt54%5Ea@9.135.97.167:25432/pcmgr_enterprise?sslmode=disable",
		ReplConnect:  "postgresql://postgres:6uYt54%5Ea@9.135.97.167:25432/pcmgr_enterprise?sslmode=disable&replication=database",
		ReplSlot:     "test",
		DecodePlugin: decode.PGOutputPlugin,
	}), sink.ConsumerOption{DebounceInterval: 6 * time.Second})

	err := consumer.Consume(map[sink.Model]sink.ModelHandlerFunc{
		&Accounts{}: func(change sink.ConsumerChange) error {
			if change.New != nil {
				fmt.Println(change.New)
			}

			if change.Old != nil {
				fmt.Println(change.Old)
			}

			fmt.Println(change.Op)
			fmt.Println(change.Checkpoint)

			return nil
		},
	})
	if err != nil {
		println(err.Error())
		return
	}
}
