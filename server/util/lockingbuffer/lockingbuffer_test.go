package lockingbuffer_test

import (
	"io/ioutil"
	"math/rand"
	"regexp"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/stretchr/testify/assert"
)

func TestLockingBuffer_ReadWrite(t *testing.T) {
	rand.Seed(int64(time.Now().UnixNano()))

	buf := lockingbuffer.New()

	writer := func(val string, writeCount int, done *bool) {
		defer func() {
			*done = true
		}()
		for i := 0; i < writeCount; i++ {
			if _, err := buf.Write([]byte(val)); err != nil {
				t.Fail()
				return
			}
			randDelay()
		}
	}

	// Writer A: Write `strACount` copies of `strA` to the buffer
	strA := "AAA"
	strACount := 1187
	writerADone := false
	go writer(strA, strACount, &writerADone)

	// Writer B: Write `strBCount` copies of `strB` to the buffer
	strB := "BBBBBBB"
	strBCount := 919
	writerBDone := false
	go writer(strB, strBCount, &writerBDone)

	reader := func(acc *string, done chan struct{}) {
		defer func() {
			done <- struct{}{}
		}()
		for {
			writersDone := writerADone && writerBDone
			b, err := ioutil.ReadAll(buf)
			if err != nil {
				t.Fail()
				return
			}
			*acc += string(b)
			if writersDone {
				break
			}
			randDelay()
		}
	}

	// Readers 1 & 2: continually attempt to read all bytes from the buffer.
	reader1Acc := ""
	reader1Done := make(chan struct{}, 1)
	go reader(&reader1Acc, reader1Done)

	reader2Acc := ""
	reader2Done := make(chan struct{}, 1)
	go reader(&reader2Acc, reader2Done)

	<-reader1Done
	<-reader2Done

	// Combined reader output should consist of exactly strACount copies of strA and
	// strBCount copies of strB (in any order).
	acc := reader1Acc + reader2Acc
	assert.Equal(t, strACount, len(regexp.MustCompile(strA).FindAllStringSubmatch(acc, -1)))
	assert.Equal(t, strBCount, len(regexp.MustCompile(strB).FindAllStringSubmatch(acc, -1)))
	assert.Equal(t, strACount*len(strA)+strBCount*len(strB), len(acc))
}

func randDelay() {
	time.Sleep(time.Microsecond * time.Duration(rand.Float64()*5))
}
