// package testproto contains utilities for working with protos in tests.
package testproto

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

// AssertEqual asserts that the expected and actual proto both have the
// same serialized contents. The test will proceed if the assertion fails.
func AssertEqual(t testing.TB, expected, actual proto.Message, msgAndArgs ...interface{}) {
	assert.Equal(t, proto.MarshalTextString(expected), proto.MarshalTextString(actual), msgAndArgs...)
}
