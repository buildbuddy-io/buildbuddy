package locking_buffer_test

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"regexp"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/locking_buffer"
)

func TestLockingBuffer_ReadWrite(t *testing.T) {
	rand.Seed(int64(time.Now().UnixNano()))

	buf := &locking_buffer.LockingBuffer{}

	strA := "AAA"
	strB := "BBBBBBB"
	n := 1000

	writer := func(val string, done *bool) {
		defer func() {
			*done = true
		}()
		for i := 0; i < n; i++ {
			if _, err := buf.Write([]byte(val)); err != nil {
				t.Fail()
				return
			}
			randDelay()
		}
	}

	// Thread 1: Write `n` copies of `strA` to the buffer
	writerADone := false
	go writer(strA, &writerADone)

	// Thread 2: Write `n` copies of `strB` to the buffer
	writerBDone := false
	go writer(strB, &writerBDone)

	// Thread 3: Continually read all bytes from the buffer
	acc := ""
	rDone := make(chan struct{}, 1)
	go func() {
		defer func() {
			rDone <- struct{}{}
		}()
		for {
			writersDone := writerADone && writerBDone

			b, err := ioutil.ReadAll(buf)
			if err != nil {
				t.Fail()
				return
			}
			acc += string(b)

			if writersDone {
				break
			}
			randDelay()
		}
	}()

	<-rDone

	// `acc` should consist of exactly n copies of strA and strB.
	strACount := len(regexp.MustCompile(strA).FindAllStringSubmatch(acc, -1))
	strBCount := len(regexp.MustCompile(strB).FindAllStringSubmatch(acc, -1))
	assert.Equal(t, n, strACount)
	assert.Equal(t, n, strBCount)
	assert.Equal(t, n*(len(strA)+len(strB)), len(acc))
}

func randDelay() {
	time.Sleep(time.Microsecond * time.Duration(rand.Float64()*5))
}
