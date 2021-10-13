package bytebufferpool_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/stretchr/testify/assert"
)

func TestBufferSize(t *testing.T) {
	bp := bytebufferpool.New(1024)

	type test struct {
		dataSize    int64
		wantBufSize int
	}
	for _, testCase := range []test{
		{dataSize: 0, wantBufSize: 1},
		{dataSize: 1, wantBufSize: 1},
		{dataSize: 8, wantBufSize: 8},
		{dataSize: 12, wantBufSize: 16},
		{dataSize: 15, wantBufSize: 16},
		{dataSize: 16, wantBufSize: 16},
		{dataSize: 17, wantBufSize: 32},
	} {
		assert.EqualValues(t, testCase.wantBufSize, len(bp.Get(testCase.dataSize)), "incorrect buffer len for length %d", testCase.dataSize)
		assert.EqualValues(t, testCase.wantBufSize, cap(bp.Get(testCase.dataSize)), "incorrect buffer cap for data of length %d", testCase.dataSize)
	}
}

func TestReuse(t *testing.T) {
	bp := bytebufferpool.New(1024)

	for i := 1; i < 20; i++ {
		bp.Put(make([]byte, i))
	}

	for i := 1; i < 30; i++ {
		buf := bp.Get(int64(i))
		assert.GreaterOrEqual(t, len(buf), i, "buffer for length %d did not have sufficient length", i)
	}
}
