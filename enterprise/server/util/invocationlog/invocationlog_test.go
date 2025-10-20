package invocationlog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWrite_APIKeySplitAcrossWrites demonstrates the security vulnerability
// where an API key split across multiple Write() calls is not properly redacted.
//
// This test SHOULD FAIL with the current implementation, proving the bug exists.
func TestWrite_APIKeySplitAcrossWrites(t *testing.T) {
	t.Skip("TODO: This test demonstrates a known security vulnerability where secrets " +
		"split across multiple Write() calls are not properly redacted. " +
		"Unskip this test when implementing line-buffered redaction to ensure the fix works.")

	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// BuildBuddy API keys are exactly 20 alphanumeric characters
	// The pattern "grpc://APIKEY@host" should be redacted
	// Split the URL so the API key spans across two Write() calls
	part1 := "grpc://US6cW4mS4WBk"        // First 12 chars of API key
	part2 := "3WXdvfkh@app.buildbuddy.io" // Last 8 chars + rest

	_, err := log.Write([]byte(part1))
	require.NoError(t, err)

	_, err = log.Write([]byte(part2))
	require.NoError(t, err)

	result := output.String()

	// The API key should be redacted even though it was split across writes
	assert.NotContains(t, result, "US6cW4mS4WBk3WXdvfkh",
		"API key should be redacted even when split across writes")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_URLPasswordSplitAcrossWrites demonstrates the vulnerability
// with URL passwords being split across writes.
func TestWrite_URLPasswordSplitAcrossWrites(t *testing.T) {
	t.Skip("TODO: This test demonstrates a known security vulnerability where secrets " +
		"split across multiple Write() calls are not properly redacted. " +
		"Unskip this test when implementing line-buffered redaction to ensure the fix works.")

	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// URL with password that should be redacted
	// Split in the middle of the password
	part1 := "https://user:mysecret"
	part2 := "password@github.com/repo"

	_, err := log.Write([]byte(part1))
	require.NoError(t, err)

	_, err = log.Write([]byte(part2))
	require.NoError(t, err)

	result := output.String()

	// The password should be redacted
	assert.NotContains(t, result, "mysecretpassword",
		"Password should be redacted even when split across writes")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_EnvVarSplitAcrossWrites demonstrates the vulnerability
// with environment variables being split across writes.
func TestWrite_EnvVarSplitAcrossWrites(t *testing.T) {
	t.Skip("TODO: This test demonstrates a known security vulnerability where secrets " +
		"split across multiple Write() calls are not properly redacted. " +
		"Unskip this test when implementing line-buffered redaction to ensure the fix works.")

	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// Environment variable with secret value
	// Split in the middle
	part1 := "--test_env=SECRET_TO"
	part2 := "KEN=my_secret_value_here"

	_, err := log.Write([]byte(part1))
	require.NoError(t, err)

	_, err = log.Write([]byte(part2))
	require.NoError(t, err)

	result := output.String()

	// The secret value should be redacted
	assert.NotContains(t, result, "my_secret_value_here",
		"Secret value should be redacted even when split across writes")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_APIKeyHeaderSplitAcrossWrites tests the x-buildbuddy-api-key header
// being split across multiple writes.
func TestWrite_APIKeyHeaderSplitAcrossWrites(t *testing.T) {
	t.Skip("TODO: This test demonstrates a known security vulnerability where secrets " +
		"split across multiple Write() calls are not properly redacted. " +
		"Unskip this test when implementing line-buffered redaction to ensure the fix works.")

	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// API key in header format
	// Split the API key value across writes
	part1 := "x-buildbuddy-api-key=12345678"
	part2 := "901234567890"

	_, err := log.Write([]byte(part1))
	require.NoError(t, err)

	_, err = log.Write([]byte(part2))
	require.NoError(t, err)

	result := output.String()

	// The API key should be redacted
	assert.NotContains(t, result, "12345678901234567890",
		"API key in header should be redacted even when split across writes")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_SecretInMultilineOutput tests a realistic scenario where
// command output contains secrets and is written in chunks that don't
// align with line boundaries.
func TestWrite_SecretInMultilineOutput(t *testing.T) {
	t.Skip("TODO: This test demonstrates a known security vulnerability where secrets " +
		"split across multiple Write() calls are not properly redacted. " +
		"Unskip this test when implementing line-buffered redaction to ensure the fix works.")

	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// Simulate command output with a secret URL on the second line
	fullOutput := "Running git clone...\n" +
		"Cloning into 'repo' from https://user:secretpass@github.com/org/repo\n" +
		"Done.\n"

	// Split at a point that cuts through the secret
	splitPoint := strings.Index(fullOutput, "secretpass") + 6 // Split in middle of "secretpass"
	part1 := fullOutput[:splitPoint]
	part2 := fullOutput[splitPoint:]

	_, err := log.Write([]byte(part1))
	require.NoError(t, err)

	_, err = log.Write([]byte(part2))
	require.NoError(t, err)

	result := output.String()

	// The password should be redacted
	assert.NotContains(t, result, "secretpass",
		"Password in multiline output should be redacted even when split across writes")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_CompleteLineRedactsCorrectly is a baseline test showing that
// when secrets are NOT split across writes, redaction works correctly.
// This test should PASS even with the buggy implementation.
func TestWrite_CompleteLineRedactsCorrectly(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// Write a complete secret in a single Write() call
	secretURL := "grpc://US6cW4mS4WBk3WXdvfkh@app.buildbuddy.io\n"

	_, err := log.Write([]byte(secretURL))
	require.NoError(t, err)

	result := output.String()

	// The API key should be redacted
	assert.NotContains(t, result, "US6cW4mS4WBk3WXdvfkh",
		"API key should be redacted in single write")

	// The redacted form should appear
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestWrite_MultipleSecretsInSingleWrite tests that multiple secrets
// in a single write are all redacted.
func TestWrite_MultipleSecretsInSingleWrite(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// Multiple secrets in one write
	text := "First secret: https://user:pass1@host1.com\n" +
		"Second secret: grpc://ABC12345678901234567@host2.com\n"

	_, err := log.Write([]byte(text))
	require.NoError(t, err)

	result := output.String()

	// Both secrets should be redacted
	assert.NotContains(t, result, "pass1", "First password should be redacted")
	assert.NotContains(t, result, "ABC12345678901234567", "Second API key should be redacted")

	// The redacted form should appear multiple times
	assert.Contains(t, result, "<REDACTED>", "Output should contain redaction placeholders")
}

// TestWrite_WriteListenerCalled verifies that the write listener
// receives the redacted output.
func TestWrite_WriteListenerCalled(t *testing.T) {
	var output bytes.Buffer
	var listenerOutput strings.Builder

	log := New()
	log.SetWriter(&output)
	log.SetWriteListener(func(s string) {
		listenerOutput.WriteString(s)
	})

	secretURL := "grpc://US6cW4mS4WBk3WXdvfkh@app.buildbuddy.io\n"

	_, err := log.Write([]byte(secretURL))
	require.NoError(t, err)

	// Listener should receive redacted output
	listenerResult := listenerOutput.String()
	assert.NotContains(t, listenerResult, "US6cW4mS4WBk3WXdvfkh",
		"Listener should receive redacted output")
	assert.Contains(t, listenerResult, "<REDACTED>",
		"Listener output should contain redaction placeholder")
}

// TestPrintln_RedactsSecrets tests that the Println helper method
// properly redacts secrets.
func TestPrintln_RedactsSecrets(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	log.Println("Connecting to", "grpc://US6cW4mS4WBk3WXdvfkh@app.buildbuddy.io")

	result := output.String()

	// The API key should be redacted
	assert.NotContains(t, result, "US6cW4mS4WBk3WXdvfkh",
		"API key should be redacted in Println")
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestPrintf_RedactsSecrets tests that the Printf helper method
// properly redacts secrets.
func TestPrintf_RedactsSecrets(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	log.Printf("Connecting to %s", "https://user:password@github.com/repo")

	result := output.String()

	// The password should be redacted
	assert.NotContains(t, result, "password",
		"Password should be redacted in Printf")
	assert.Contains(t, result, "<REDACTED>",
		"Output should contain redaction placeholder")
}

// TestNew_LockingBufferIsPopulated verifies that the embedded LockingBuffer
// receives data even when using a custom writer.
func TestNew_LockingBufferIsPopulated(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	testData := "Test log entry with https://user:secret@host.com\n"
	_, err := log.Write([]byte(testData))
	require.NoError(t, err)

	// The custom writer should receive redacted data
	customWriterOutput := output.String()
	assert.NotContains(t, customWriterOutput, "secret",
		"Custom writer should receive redacted output")

	// The embedded LockingBuffer should also receive redacted data
	lockingBufferOutput := log.String()
	assert.NotContains(t, lockingBufferOutput, "secret",
		"LockingBuffer should receive redacted output")
	assert.Contains(t, lockingBufferOutput, "<REDACTED>",
		"LockingBuffer should contain redacted output")

	// Both outputs should be identical
	assert.Equal(t, customWriterOutput, lockingBufferOutput,
		"Both custom writer and LockingBuffer should have the same content")
}

// TestNew_LockingBufferMethods verifies that all LockingBuffer methods work correctly.
func TestNew_LockingBufferMethods(t *testing.T) {
	var output bytes.Buffer
	log := New()
	log.SetWriter(&output)

	// Write some data
	log.Println("Line 1")
	log.Println("Line 2")

	// Test Len()
	assert.Greater(t, log.Len(), 0, "Len() should return non-zero")

	// Test String()
	str := log.String()
	assert.Contains(t, str, "Line 1", "String() should contain written data")
	assert.Contains(t, str, "Line 2", "String() should contain written data")

	// Test ReadAll()
	data, err := log.ReadAll()
	require.NoError(t, err)
	assert.Contains(t, string(data), "Line 1", "ReadAll() should return written data")
	assert.Contains(t, string(data), "Line 2", "ReadAll() should return written data")

	// After ReadAll(), the buffer should be empty
	assert.Equal(t, 0, log.Len(), "Len() should be 0 after ReadAll()")
}

// TestNew_DefaultBehavior verifies that New() works with default settings
// (writing to os.Stderr and LockingBuffer).
func TestNew_DefaultBehavior(t *testing.T) {
	log := New()

	// Write some data (will go to stderr by default, but we can still check LockingBuffer)
	log.Println("Test message")

	// The LockingBuffer should have received the data
	assert.Greater(t, log.Len(), 0, "LockingBuffer should contain data")
	assert.Contains(t, log.String(), "Test message", "LockingBuffer should contain the message")
}

// TestSetWriter_ReplacesDestination verifies that SetWriter properly changes
// the output destination while keeping LockingBuffer populated.
func TestSetWriter_ReplacesDestination(t *testing.T) {
	var firstWriter bytes.Buffer
	var secondWriter bytes.Buffer

	log := New()
	log.SetWriter(&firstWriter)

	// Write to first writer
	log.Println("Message 1")

	// Switch to second writer
	log.SetWriter(&secondWriter)

	// Write to second writer
	log.Println("Message 2")

	// First writer should only have Message 1
	assert.Contains(t, firstWriter.String(), "Message 1")
	assert.NotContains(t, firstWriter.String(), "Message 2")

	// Second writer should only have Message 2
	assert.NotContains(t, secondWriter.String(), "Message 1")
	assert.Contains(t, secondWriter.String(), "Message 2")

	// LockingBuffer should have both messages
	lockingBufferContent := log.String()
	assert.Contains(t, lockingBufferContent, "Message 1")
	assert.Contains(t, lockingBufferContent, "Message 2")
}
