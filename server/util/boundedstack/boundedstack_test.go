package boundedstack_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/boundedstack"
	"github.com/stretchr/testify/require"
)

func TestBoundedStack_FillAndDrain(t *testing.T) {
	s, err := boundedstack.New[int](2 /*=capacity*/)
	require.NoError(t, err)

	s.Push(1)
	s.Push(2)

	<-s.C
	n, ok := s.Pop()
	require.True(t, ok)
	require.Equal(t, 2, n)
	<-s.C
	n, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 1, n)

	select {
	case <-s.C:
		require.FailNow(t, "channel should be drained")
	default:
	}
	_, ok = s.Pop()
	require.False(t, ok)
}

func TestBoundedStack_OverfillAndDrain(t *testing.T) {
	s, err := boundedstack.New[int](2 /*=capacity*/)
	require.NoError(t, err)

	s.Push(1)
	s.Push(2)
	s.Push(3) // should evict 1

	<-s.C
	n, ok := s.Pop()
	require.True(t, ok)
	require.Equal(t, 3, n)
	<-s.C
	n, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 2, n)

	select {
	case <-s.C:
		require.FailNow(t, "channel should be drained")
	default:
	}
	_, ok = s.Pop()
	require.False(t, ok)
}
