package dblog

import (
	"errors"
	"sync"
	"time"

	"github.com/replicase/pgcapture/pkg/pb"
)

type OnSchedule func(response *pb.DumpInfoResponse) error
type AfterSchedule func()
type CancelFunc func()

var ErrAlreadyScheduled = errors.New("already scheduled")
var ErrAlreadyRegistered = errors.New("already registered")

type Scheduler interface {
	Schedule(uri string, dumps []*pb.DumpInfoResponse, fn AfterSchedule) error //
	Register(uri string, client string, fn OnSchedule) (CancelFunc, error)     //注册接收端
	Ack(uri string, client string, requeue string)                             //下发任务
	SetCoolDown(uri string, dur time.Duration)
	StopSchedule(uri string)
}

func NewMemoryScheduler(interval time.Duration) *MemoryScheduler {
	return &MemoryScheduler{
		interval: interval,
		pending:  make(map[string]*pending),
		clients:  make(map[string]map[string]*track),
	}
}

// MemoryScheduler 任务调度
type MemoryScheduler struct {
	interval  time.Duration                //调度间隔
	pending   map[string]*pending          //任务队列
	clients   map[string]map[string]*track //接收端
	pendingMu sync.Mutex
	clientsMu sync.Mutex
}

/*
fn = func() {log.Infof("finish scheduling dumps of %s", req.Uri)}
*/

// Schedule 调度任务
func (s *MemoryScheduler) Schedule(uri string, dumps []*pb.DumpInfoResponse, fn AfterSchedule) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	if _, ok := s.pending[uri]; ok {
		return ErrAlreadyScheduled
	}
	s.pending[uri] = &pending{dumps: dumps}

	//调用该uri下的所有任务
	go s.schedule(uri, fn)
	return nil
}

func (s *MemoryScheduler) schedule(uri string, fn AfterSchedule) {
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, uri)
		s.pendingMu.Unlock()
		fn()
	}()

	for {
		time.Sleep(s.interval)

		loops := 0
		s.clientsMu.Lock()
		loops = len(s.clients[uri])
		s.clientsMu.Unlock()
		loops++

		for i := 0; i < loops; i++ {
			if s.scheduleOne(uri) {
				return
			}
		}
	}
}

// 一个个发送dump info 到端
func (s *MemoryScheduler) scheduleOne(uri string) bool {
	var candidate *track
	var dump *pb.DumpInfoResponse

	busy := 0
	remain := 0
	stopping := false

	s.clientsMu.Lock()
	if clients, ok := s.clients[uri]; ok {
		for _, track := range clients {
			if track.dump == nil {
				candidate = track
			} else {
				busy++
			}
		}
	}
	s.clientsMu.Unlock()

	s.pendingMu.Lock()
	if pending, ok := s.pending[uri]; ok {
		remain = pending.Remaining()
		stopping = pending.stopping
		coolDown := pending.CoolDown()
		if candidate != nil && !stopping &&
			(coolDown == 0 || time.Now().Sub(candidate.ackTs) > coolDown) {
			dump = pending.Pop()
		}
	}
	s.pendingMu.Unlock()

	if candidate != nil && dump != nil {
		s.clientsMu.Lock()
		candidate.dump = dump
		s.clientsMu.Unlock()
		if err := candidate.schedule(dump); err != nil {
			candidate.cancel()
		}
	}

	if stopping {
		return busy == 0
	}

	return busy == 0 && remain == 0
}

// Register 向调度器注册接收端和处理函数
func (s *MemoryScheduler) Register(uri string, client string, fn OnSchedule) (CancelFunc, error) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	clients, ok := s.clients[uri]
	if !ok {
		clients = make(map[string]*track)
		s.clients[uri] = clients
	}
	if _, ok = clients[client]; ok {
		return nil, ErrAlreadyRegistered
	}
	track := &track{schedule: fn, cancel: func() {
		s.Ack(uri, client, "canceled")
		s.clientsMu.Lock()
		delete(clients, client)
		s.clientsMu.Unlock()
	}}
	clients[client] = track

	return track.cancel, nil
}

// Ack 向调度下发任务
func (s *MemoryScheduler) Ack(uri string, client string, requeue string) {
	var dump *pb.DumpInfoResponse

	s.clientsMu.Lock()
	if clients, ok := s.clients[uri]; ok {
		if track, ok := clients[client]; ok {
			dump = track.dump
			track.dump = nil
			track.ackTs = time.Now()
		}
	}
	s.clientsMu.Unlock()

	if dump != nil {
		s.pendingMu.Lock()
		if pending, ok := s.pending[uri]; ok {
			if requeue != "" {
				pending.Push(dump)
				pending.backoff++
			} else {
				pending.backoff = 0
			}
		}
		s.pendingMu.Unlock()
	}
}

func (s *MemoryScheduler) SetCoolDown(uri string, dur time.Duration) {
	s.pendingMu.Lock()
	if pending, ok := s.pending[uri]; ok {
		pending.coolDown = dur
	}
	s.pendingMu.Unlock()
}

func (s *MemoryScheduler) StopSchedule(uri string) {
	s.pendingMu.Lock()
	if pending, ok := s.pending[uri]; ok {
		pending.stopping = true
	}
	s.pendingMu.Unlock()
}

type track struct {
	dump     *pb.DumpInfoResponse
	ackTs    time.Time
	schedule OnSchedule
	cancel   CancelFunc
}

type pending struct {
	dumps    []*pb.DumpInfoResponse
	coolDown time.Duration
	backoff  int
	offset   int
	stopping bool
}

const (
	backoffGap time.Duration = 2
	backoffMax               = 8
)

func (p *pending) CoolDown() time.Duration {
	if p.backoff == 0 {
		return p.coolDown
	}
	step := backoffGap
	for i := 0; i < backoffMax && i < p.backoff-1; i++ {
		step *= 2
	}
	return p.coolDown + (step * time.Second)
}

func (p *pending) Remaining() int {
	return len(p.dumps) - p.offset
}

func (p *pending) Pop() *pb.DumpInfoResponse {
	if len(p.dumps) == p.offset {
		return nil
	}
	ret := p.dumps[p.offset]
	p.offset++
	return ret
}

func (p *pending) Push(dump *pb.DumpInfoResponse) {
	if p.offset == 0 {
		return
	}
	p.offset--
	p.dumps[p.offset] = dump
}
