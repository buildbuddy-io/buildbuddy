package cachelog

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const enabled = false

var (
	initOnce sync.Once
	msgs     chan *logMsg
)

const (
	ByteStreamRead  Op = "SR"
	ByteStreamWrite Op = "SW"
	BatchRead       Op = "BR"
	BatchWrite      Op = "BW"
)

type Op string

type logMsg struct {
	id   string
	op   Op
	hash string
	size int64
}

func initialize() {
	initOnce.Do(func() {
		f, err := os.OpenFile("/tmp/buildbuddy_cache.log", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0777)
		if err != nil {
			log.Errorf("Failed to open cache log: %s", err)
		}
		msgs = make(chan *logMsg, 512)
		go func() {
			for m := range msgs {
				_, err := f.Write([]byte(fmt.Sprintf(`{"t":%d,"id":%q,"op":%q,"hash":%q,"size":%d}`, time.Now().UnixNano(), m.id, m.op, m.hash, m.size)))
				if err != nil {
					log.Errorf("Failed to write to CacheLog: %s", err)
				}
			}
		}()
	})
}

func Log(ctx context.Context, op Op, hash string, sizeBytes int64) {
	if !enabled {
		return
	}
	initialize()
	id, ok := ctx.Value("uuid").(string)
	if !ok {
		id = ""
	}
	select {
	case msgs <- &logMsg{id: id, op: op, hash: hash, size: sizeBytes}:
	default:
		log.Warningf("Failed to write to cachelog (buffer not draining fast enough)")
	}
}
