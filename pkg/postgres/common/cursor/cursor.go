package cursor

import (
	"errors"
	"github.com/jackc/pglogrepl"
	"strconv"
	"strings"
)

type Checkpoint struct {
	LSN  uint64
	Seq  uint32
	Data []byte
}

func (cp *Checkpoint) Equal(cp2 Checkpoint) bool {
	return cp.LSN == cp2.LSN && cp.Seq == cp2.Seq
}

func (cp *Checkpoint) After(cp2 Checkpoint) bool {
	return (cp.LSN > cp2.LSN) || (cp.LSN == cp2.LSN && cp.Seq > cp2.Seq)
}

func (cp *Checkpoint) ToKey() string {
	return pglogrepl.LSN(cp.LSN).String() + "|" + strconv.FormatUint(uint64(cp.Seq), 16)
}

func (cp *Checkpoint) FromKey(str string) error {
	parts := strings.Split(str, "|")
	if len(parts) != 2 {
		return errors.New("malformed key, should be lsn|seq")
	}
	lsn, err := pglogrepl.ParseLSN(parts[0])
	if err != nil {
		return err
	}
	seq, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return err
	}
	cp.LSN = uint64(lsn)
	cp.Seq = uint32(seq)
	return nil
}
