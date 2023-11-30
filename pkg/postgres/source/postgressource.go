package source

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"github.com/jackc/pgx/v5"
	"github.com/replicase/pgcapture/pkg/postgres/common/cursor"
	decode2 "github.com/replicase/pgcapture/pkg/postgres/common/decode"
	"github.com/replicase/pgcapture/pkg/postgres/common/sql"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sirupsen/logrus"
)

type PGXSourceOption struct {
	SetupConnect string
	ReplConnect  string
	ReplSlot     string
	DecodePlugin string
}

// PGXSource 实现了与PG的逻辑复制功能
type PGXSource struct {
	BaseSource

	SetupConnStr      string
	ReplConnStr       string
	ReplSlot          string
	CreateSlot        bool
	CreatePublication bool
	StartLSN          string
	DecodePlugin      string

	setupConn      *pgx.Conn
	replConn       *pgconn.PgConn
	schema         *decode2.PGXSchemaLoader
	decoder        decode2.Decoder //实现了内部表示到外部表示的转码
	nextReportTime time.Time
	ackLsn         uint64
	txCounter      uint64
	log            *logrus.Entry
	isNotFirst     bool
	currentLsn     uint64
	currentSeq     uint32
}

func NewPGXSource(options PGXSourceOption) *PGXSource {
	source := &PGXSource{
		BaseSource:        BaseSource{},
		SetupConnStr:      options.SetupConnect,
		ReplConnStr:       options.ReplConnect,
		ReplSlot:          options.ReplSlot,
		CreateSlot:        false,
		CreatePublication: false,
		StartLSN:          "",
		DecodePlugin:      options.DecodePlugin,
		setupConn:         nil,
		replConn:          nil,
		schema:            nil,
		decoder:           nil,
		nextReportTime:    time.Time{},
		ackLsn:            0,
		txCounter:         0,
		log:               nil,
		isNotFirst:        false,
		currentLsn:        0,
		currentSeq:        0,
	}
	if len(options.ReplSlot) != 0 {
		source.CreateSlot = true
		source.CreatePublication = true
	}
	return source
}

func (p *PGXSource) TxCounter() uint64 {
	return atomic.LoadUint64(&p.txCounter)
}

func (p *PGXSource) Capture(cp cursor.Checkpoint) (changes chan Change, err error) {
	defer func() {
		if err != nil {
			p.cleanup()
		}
	}()

	ctx := context.Background()
	p.setupConn, err = pgx.Connect(ctx, p.SetupConnStr)
	if err != nil {
		return nil, err
	}

	// 不需要监控ddl，注释掉
	//if _, err = p.setupConn.Exec(ctx, sql.InstallExtension); err != nil {
	//	return nil, err
	//}

	//更新schema缓存
	p.schema = decode2.NewPGXSchemaLoader(p.setupConn)
	if err = p.schema.RefreshType(); err != nil {
		return nil, err
	}

	switch p.DecodePlugin {
	case decode2.PGLogicalOutputPlugin:
		p.decoder, err = decode2.NewPGLogicalDecoder(p.schema)
		if err != nil {
			return nil, err
		}
	case decode2.PGOutputPlugin:
		p.decoder = decode2.NewPGOutputDecoder(p.schema, p.ReplSlot)
		if p.CreatePublication {
			if _, err = p.setupConn.Exec(ctx, fmt.Sprintf(sql.CreatePublication, p.ReplSlot)); err != nil {
				var pge *pgconn.PgError
				if !errors.As(err, &pge) || pge.Code != "42710" {
					return nil, err
				}
			}
		}
	default:
		return nil, errors.New("unknown decode plugin")
	}

	if p.CreateSlot {
		if _, err = p.setupConn.Exec(ctx, sql.CreateLogicalSlot, p.ReplSlot, p.DecodePlugin); err != nil {
			var pge *pgconn.PgError
			if !errors.As(err, &pge) || pge.Code != "42710" {
				return nil, err
			}
		}
	}

	// 为复制做准备
	p.replConn, err = pgconn.Connect(context.Background(), p.ReplConnStr)
	if err != nil {
		return nil, err
	}

	ident, err := pglogrepl.IdentifySystem(context.Background(), p.replConn)
	if err != nil {
		return nil, err
	}

	p.log = logrus.WithFields(logrus.Fields{"From": "PGXSource"})
	p.log.WithFields(logrus.Fields{
		"SystemID": ident.SystemID,
		"Timeline": ident.Timeline,
		"XLogPos":  int64(ident.XLogPos),
		"DBName":   ident.DBName,
		"Decoder":  p.DecodePlugin,
	}).Info("retrieved current info of source database")

	if cp.LSN != 0 {
		p.currentLsn = cp.LSN
		p.currentSeq = cp.Seq
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from requested position")
	} else {
		if p.StartLSN != "" {
			startLsn, err := pglogrepl.ParseLSN(p.StartLSN)
			if err != nil {
				return nil, err
			}
			p.currentLsn = uint64(startLsn)
		} else {
			p.currentLsn = uint64(ident.XLogPos)
		}
		p.currentSeq = 0
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from the latest position")
	}

	p.Commit(cursor.Checkpoint{LSN: p.currentLsn})
	if err = pglogrepl.StartReplication(
		context.Background(),
		p.replConn,
		p.ReplSlot,
		pglogrepl.LSN(p.currentLsn),
		pglogrepl.StartReplicationOptions{PluginArgs: p.decoder.GetPluginArgs()},
	); err != nil {
		return nil, err
	}

	return p.BaseSource.capture(p.fetching, p.cleanup)
}

// Commit 更新check point到PGXSource
func (p *PGXSource) Commit(cp cursor.Checkpoint) {
	if cp.LSN != 0 {
		atomic.StoreUint64(&p.ackLsn, cp.LSN)
		atomic.AddUint64(&p.txCounter, 1)
	}
}

func (p *PGXSource) Requeue(cp cursor.Checkpoint, reason string) {
	log.Infof("%v %v", cp, reason)
}

func (p *PGXSource) committedLSN() (lsn pglogrepl.LSN) {
	return pglogrepl.LSN(atomic.LoadUint64(&p.ackLsn))
}

// 向postgres发送committedLSN
func (p *PGXSource) reportLSN(ctx context.Context) error {
	if committed := p.committedLSN(); committed != 0 {
		return pglogrepl.SendStandbyStatusUpdate(ctx, p.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: committed})
	}
	return nil
}

func (p *PGXSource) fetching(ctx context.Context) (change Change, err error) {

	// 定时发送committedLSN
	if time.Now().After(p.nextReportTime) {
		if err = p.reportLSN(ctx); err != nil {
			return change, err
		}
		p.nextReportTime = time.Now().Add(5 * time.Second)
	}

	// 如果没有消息可用，该方法将会阻塞当前的 goroutine，直到接收到消息为止
	msg, err := p.replConn.ReceiveMessage(ctx)
	if err != nil {
		return change, err
	}

	/*
		XLogDataByteID：这个常量代表 'w'，在 PostgresSQL 的复制协议中，它表示一个 WAL 数据消息（XLogData 消息）。
		这种消息包含了 WAL 数据的有效负载。
		PrimaryKeepaliveMessageByteID：这个常量代表 'k'，在 PostgresSQL 的复制协议中，它表示一个主服务器的保活消息（PrimaryKeepaliveMessage）。
		这种消息由主服务器定期发送，用于告知备份服务器主服务器的状态。
		StandbyStatusUpdateByteID：这个常量代表 'r'，在 PostgresSQL 的复制协议中，它表示一个备份服务器状态更新消息（StandbyStatusUpdate）。
		这种消息由备份服务器发送，用于告知主服务器备份服务器的状态。
	*/
	// 接收到新的消息
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			/*
				ServerWALEnd：这是一个LSN类型的字段，表示主服务器的WAL（Write-Ahead Logging）结束位置。
				LSN（Log Sequence Number）是PostgreSQL中用于标识WAL位置的数据类型。
				ServerTime：这是一个time.Time类型的字段，表示主服务器发送保活消息时的服务器时间。
				ReplyRequested：这是一个bool类型的字段，表示主服务器是否请求备份服务器回复。
				如果为true，则备份服务器需要尽快发送一个Standby Status Update消息。
			*/
			var pkm pglogrepl.PrimaryKeepaliveMessage
			if pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:]); err == nil && pkm.ReplyRequested {
				p.nextReportTime = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			/*
				WALStart：这是一个LSN类型的字段，表示WAL数据开始的位置。LSN（Log Sequence Number）是PostgreSQL中用于标识WAL位置的数据类型。
				ServerWALEnd：这是一个LSN类型的字段，表示主服务器的WAL结束位置。
				ServerTime：这是一个time.Time类型的字段，表示主服务器发送WAL数据消息时的服务器时间。
				WALData：这是一个[]byte类型的字段，表示WAL数据的有效负载。这个字段包含了实际的WAL数据。
			*/
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return change, err
			}
			// in the implementation of pgx v5, the xld.WALData will be reused
			walData := make([]byte, len(xld.WALData))
			copy(walData, xld.WALData)
			m, err := p.decoder.Decode(walData)
			if m == nil || err != nil {
				return change, err
			}
			if msg := m.GetChange(); msg != nil {
				if decode2.Ignore(msg) {
					return change, nil
				} else if decode2.IsDDL(msg) {
					if err = p.schema.RefreshType(); err != nil {
						return change, err
					}
				}
				p.currentSeq++
			} else if b := m.GetBegin(); b != nil {
				p.currentLsn = b.FinalLsn
				p.currentSeq = 0
			} else if c := m.GetCommit(); c != nil {
				p.currentLsn = c.CommitLsn
				p.currentSeq++
			}
			change = Change{
				Checkpoint: cursor.Checkpoint{LSN: p.currentLsn, Seq: p.currentSeq},
				Message:    m,
			}
			if !p.isNotFirst {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"Message":    m.String(),
				}).Info("retrieved the first message from postgres")
				p.isNotFirst = true
			}
		}
	default:
		err = errors.New("unexpected message")
	}
	return change, err
}

func (p *PGXSource) cleanup() {
	ctx := context.Background()
	if p.setupConn != nil {
		err := p.setupConn.Close(ctx)
		if err != nil {
			log.Errorf("close connection to postgres failed,error:%v", err)
		}
	}
	if p.replConn != nil {
		err := p.reportLSN(ctx)
		if err != nil {
			log.Errorf("reportLSN failed,error:%v")
		}
		err = p.replConn.Close(ctx)
		if err != nil {
			log.Errorf("close connection to postgres failed,error:%v", err)
		}
	}
}
