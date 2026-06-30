package install_mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseClientNames(t *testing.T) {
	require.Equal(t, []string{"codex", "claude"}, parseClientNames(" Codex,claude ,, "))
	require.Nil(t, parseClientNames("   "))
}

func TestJSONRPCError(t *testing.T) {
	response := jsonRPCError([]byte(`{"jsonrpc":"2.0","id":7,"method":"initialize"}`), -32603, "boom")
	var parsed struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(response, &parsed))
	require.Equal(t, "2.0", parsed.JSONRPC)
	require.Equal(t, 7, parsed.ID)
	require.Equal(t, -32603, parsed.Error.Code)
	require.Equal(t, "boom", parsed.Error.Message)
}

func TestJSONRPCErrorNotification(t *testing.T) {
	require.Nil(t, jsonRPCError([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`), -32603, "boom"))
	require.Nil(t, jsonRPCError([]byte(`{"jsonrpc":"2.0","id":null,"method":"notifications/initialized"}`), -32603, "boom"))
}

func TestRunMCPProxy(t *testing.T) {
	t.Setenv(mcpAPIKeyEnvVar, "test-api-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := RunMCPProxy(context.Background(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`+"\n"), &stdout, &stderr, server.URL)
	require.NoError(t, err)
	require.Empty(t, stderr.String())
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`, strings.TrimSpace(stdout.String()))
}

func TestRunMCPProxyNotificationNoResponse(t *testing.T) {
	t.Setenv(mcpAPIKeyEnvVar, "test-api-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := RunMCPProxy(context.Background(), strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`+"\n"), &stdout, &stderr, server.URL)
	require.NoError(t, err)
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}
