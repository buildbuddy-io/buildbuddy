package lockingbuffer_test

import (
	"io"
	"math/rand"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/stretchr/testify/assert"
)

func TestLockingBuffer_ReadWrite(t *testing.T) {
	rand.Seed(int64(time.Now().UnixNano()))

	buf := lockingbuffer.New()

	writer := func(val string, writeCount int, done *bool, doneLock *sync.RWMutex) {
		defer func() {
			doneLock.Lock()
			*done = true
			doneLock.Unlock()
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
	doneLock := sync.RWMutex{}
	go writer(strA, strACount, &writerADone, &doneLock)

	// Writer B: Write `strBCount` copies of `strB` to the buffer
	strB := "BBBBBBB"
	strBCount := 919
	writerBDone := false
	go writer(strB, strBCount, &writerBDone, &doneLock)

	reader := func(acc *string, done chan struct{}) {
		defer func() {
			done <- struct{}{}
		}()
		chunk := make([]byte, 128)
		for {
			doneLock.RLock()
			writersDone := writerADone && writerBDone
			doneLock.RUnlock()
			n, err := buf.Read(chunk)
			if err != nil && err != io.EOF {
				t.Error(err)
				return
			}
			*acc += string(chunk[:n])
			if writersDone && n == 0 {
				return
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

	// Assert that we read the correct number of 'A' and 'B'.
	acc := reader1Acc + reader2Acc
	assert.Equal(t, strACount*len(strA), charCount(acc, 'A'))
	assert.Equal(t, strBCount*len(strB), charCount(acc, 'B'))
	assert.Equal(t, strACount*len(strA)+strBCount*len(strB), len(acc))
}

func charCount(s string, c byte) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			n++
		}
	}
	return n
}

func TestLockingBuffer_ReadAll(t *testing.T) {
	rand.Seed(int64(time.Now().UnixNano()))

	buf := lockingbuffer.New()

	writer := func(val string, writeCount int, done *bool, doneLock *sync.RWMutex) {
		defer func() {
			doneLock.Lock()
			*done = true
			doneLock.Unlock()
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
	doneLock := sync.RWMutex{}
	go writer(strA, strACount, &writerADone, &doneLock)

	// Writer B: Write `strBCount` copies of `strB` to the buffer
	strB := "BBBBBBB"
	strBCount := 919
	writerBDone := false
	go writer(strB, strBCount, &writerBDone, &doneLock)

	reader := func(acc *string, done chan struct{}) {
		defer func() {
			done <- struct{}{}
		}()
		for {
			doneLock.RLock()
			writersDone := writerADone && writerBDone
			doneLock.RUnlock()
			b, err := buf.ReadAll()
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
