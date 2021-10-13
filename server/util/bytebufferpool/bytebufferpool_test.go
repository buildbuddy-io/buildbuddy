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
		assert.Equal(t, testCase.wantBufSize, len(bp.Get(testCase.dataSize)), "incorrect buffer size for data of length %d", testCase.dataSize)
	}
}
