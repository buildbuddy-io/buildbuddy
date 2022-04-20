package paging_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
)

func TestDecodeAndEncodeOffsetLimit(t *testing.T) {
	in := &pgpb.OffsetLimit{Offset: 1000, Limit: 100}

	str, err := paging.EncodeOffsetLimit(in)
	require.NoError(t, err)
	out, err := paging.DecodeOffsetLimit(str)
	require.NoError(t, err)

	assert.Equal(t, in.Offset, out.Offset, "unexpected Offset")
	assert.Equal(t, in.Limit, out.Limit, "unexpected Limit")
}
