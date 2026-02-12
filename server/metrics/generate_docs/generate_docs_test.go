package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLabelConstantWithSingleSpace(t *testing.T) {
	gen := newDocsGenerator("/dev/null")
	gen.state = "LABEL_CONSTANTS"
	err := gen.processLine(0, `    StatusLabel = "status"`)
	require.NoError(t, err)
	constInfo, ok := gen.labelConstants["StatusLabel"]
	require.True(t, ok, "expected StatusLabel to be in labelConstants")
	assert.Equal(t, "status", constInfo.value)
}

func TestParseLabelConstantWithMultipleSpaces(t *testing.T) {
	gen := newDocsGenerator("/dev/null")
	gen.state = "LABEL_CONSTANTS"
	err := gen.processLine(0, `    StatusHumanReadableLabel         = "status"`)
	require.NoError(t, err)
	constInfo, ok := gen.labelConstants["StatusHumanReadableLabel"]
	require.True(t, ok, "expected StatusHumanReadableLabel to be in labelConstants")
	assert.Equal(t, "status", constInfo.value)
}
