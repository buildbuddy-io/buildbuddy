package boundedstack

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedStack_FillAndDrain(t *testing.T) {
	ctx := context.Background()
	s, err := New[int](2 /*=capacity*/)
	require.NoError(t, err)

	s.Push(1)
	s.Push(2)

	n, err := s.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	n, err = s.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	_, ok := s.pop()
	require.False(t, ok, "stack should be empty")
}

func TestBoundedStack_OverfillAndDrain(t *testing.T) {
	ctx := context.Background()
	s, err := New[int](2 /*=capacity*/)
	require.NoError(t, err)

	s.Push(1)
	s.Push(2)
	s.Push(3) // should evict 1

	n, err := s.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	n, err = s.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	_, ok := s.pop()
	require.False(t, ok, "stack should be empty")
}
