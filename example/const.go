package example

import "github.com/replicase/pgcapture/internal/test"

const (
	PGHost      = "127.0.0.1"
	PulsarURL   = "pulsar://127.0.0.1:6650"
	TestTable   = "test"
	TestDBSrc   = "db_src"
	TestDBSink  = "db_sink"
	AgentAddr1  = "localhost:8888"
	AgentAddr2  = "localhost:8889"
	ControlAddr = "localhost:10000"
	GatewayAddr = "localhost:10001"
)

var (
	DefaultDB = test.DBURL{Host: PGHost, DB: "postgres"}
	SinkDB    = test.DBURL{Host: PGHost, DB: TestDBSink}
	SrcDB     = test.DBURL{Host: PGHost, DB: TestDBSrc}
)
