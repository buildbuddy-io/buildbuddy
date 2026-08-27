package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCopyRule(t *testing.T) {
	originalImage := *containerImage
	t.Cleanup(func() { *containerImage = originalImage })
	*containerImage = `docker://example.com/image"with-quote`

	rule, err := createCopyRule("target", []string{"input1", "input2"}, []string{"output1", "output2"})
	require.NoError(t, err)
	assert.Contains(t, rule, `srcs = ["input1","input2"]`)
	assert.Contains(t, rule, `outs = ["output1","output2"]`)
	assert.Contains(t, rule, `"container-image": "docker://example.com/image\"with-quote"`)
	assert.Contains(t, rule, `"salt": "`)
}

func TestCreateCopyRule_InputOutputCountMismatch(t *testing.T) {
	_, err := createCopyRule("target", []string{"input"}, nil)
	require.Error(t, err)
}
