package relaywire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

func TestConnect_SuccessThenStreamCarriesBytes(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	gotReq := make(chan *gwpb.RelayRequest, 1)
	go func() {
		defer server.Close()
		req, err := ReadRequest(server)
		if err != nil {
			t.Error(err)
			return
		}
		gotReq <- req
		if err := Accept(server, "10.0.0.7:4317"); err != nil {
			t.Error(err)
			return
		}
		// The handshake framing must end exactly at the response: everything
		// after it belongs to the target's stream, byte for byte.
		io.Copy(server, server)
	}()

	require.NoError(t, Connect(client, "otel.svc.cluster.local", 4317))
	req := <-gotReq
	require.Equal(t, "otel.svc.cluster.local", req.GetHost())
	require.Equal(t, int32(4317), req.GetPort())

	const payload = "spliced bytes"
	go io.WriteString(client, payload)
	buf := make([]byte, len(payload))
	_, err := io.ReadFull(client, buf)
	require.NoError(t, err)
	require.Equal(t, payload, string(buf))
}

func TestConnect_RefusalCarriesTheRelaysExplanation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	const why = `target "db.prod.internal:5432" is not in the relay's allowed suffix list`
	go func() {
		defer server.Close()
		if _, err := ReadRequest(server); err != nil {
			t.Error(err)
			return
		}
		Refuse(server, status.PermissionDeniedError(why))
	}()

	err := Connect(client, "db.prod.internal", 5432)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "got %v", err)
	require.Equal(t, why, status.Message(err))
	require.ErrorContains(t, err, why, "the relay's explanation must survive into the error text")

	wrapped := fmt.Errorf("dialing through relay: %w", err)
	require.True(t, status.IsPermissionDeniedError(wrapped), "callers detect relay verdicts through wrapping")
}

func TestConnect_RejectsInvalidPortBeforeWriting(t *testing.T) {
	for _, port := range []int{0, -1, 65536} {
		// A Write on this pipe would block forever with nobody reading, so
		// returning at all proves the port was rejected up front.
		client, _ := net.Pipe()
		require.Error(t, Connect(client, "host", port), "port %d", port)
		client.Close()
	}
}

func TestReadRequest_RejectsMalformedRequests(t *testing.T) {
	for name, req := range map[string]*gwpb.RelayRequest{
		"no host":        {Port: 443},
		"zero port":      {Host: "h"},
		"port too large": {Host: "h", Port: 65536},
	} {
		var buf bytes.Buffer
		require.NoError(t, writeFrame(&buf, req))
		_, err := ReadRequest(&buf)
		require.Error(t, err, name)
	}
}

func TestReadFrame_RefusesOversizedFrames(t *testing.T) {
	var buf bytes.Buffer
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], maxFrameSize+1)
	buf.Write(prefix[:])

	_, err := ReadRequest(&buf)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds")
}
