package bytebufferpool_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/stretchr/testify/assert"
)

func TestBufferSize(t *testing.T) {
	bp := bytebufferpool.VariableSize(1024)

	type test struct {
		dataSize   int64
		wantBufLen int
		wantBufCap int
	}
	for _, testCase := range []test{
		{dataSize: 0, wantBufLen: 0, wantBufCap: 1},
		{dataSize: 1, wantBufLen: 1, wantBufCap: 1},
		{dataSize: 8, wantBufLen: 8, wantBufCap: 8},
		{dataSize: 12, wantBufLen: 12, wantBufCap: 16},
		{dataSize: 15, wantBufLen: 15, wantBufCap: 16},
		{dataSize: 16, wantBufLen: 16, wantBufCap: 16},
		{dataSize: 17, wantBufLen: 17, wantBufCap: 32},
		{dataSize: 1024, wantBufLen: 1024, wantBufCap: 1024},
		{dataSize: 1025, wantBufLen: 1024, wantBufCap: 1024},
	} {
		assert.EqualValues(t, testCase.wantBufLen, len(bp.Get(testCase.dataSize)), "incorrect buffer len for length %d", testCase.dataSize)
		assert.EqualValues(t, testCase.wantBufCap, cap(bp.Get(testCase.dataSize)), "incorrect buffer cap for data of length %d", testCase.dataSize)
	}
}

func TestReuse(t *testing.T) {
	bp := bytebufferpool.VariableSize(1024)

	for i := 1; i < 20; i++ {
		bp.Put(make([]byte, i))
	}

	for i := 1; i < 30; i++ {
		buf := bp.Get(int64(i))
		assert.GreaterOrEqual(t, len(buf), i, "buffer for length %d did not have sufficient length", i)
	}
}
