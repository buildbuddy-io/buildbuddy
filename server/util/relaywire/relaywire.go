// Package relaywire implements the relay protocol used by the gateway
// relay service.
//
// The client sends a raw RelayRequest proto, the server processes it and sends
// raw bytes containing a RelayResponse proto. If the relay request is accepted
// the raw bytes between the client and the destination are shuffled until both
// ends of the connection are closed.
package relaywire

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

// DefaultPort is the TCP port the gateway relay listens on, on each network's
// hub IP, and tunnel clients dial.
const DefaultPort = 1081

// maxFrameSize is the maximum size of the relay request proto sent at the
// beginning of the connection.
const maxFrameSize = 4 * 1024

// Connect performs the client half of the handshake over an established
// stream. On success the stream carries the target's bytes. If the relay
// refuses the request, the returned error is a gRPC status error carrying the
// relay's code and message, so callers can check it with the status package's
// Is*Error helpers and read the explanation with status.Message.
func Connect(rw io.ReadWriter, host string, port int) error {
	if port <= 0 || port > 0xFFFF {
		return fmt.Errorf("relaywire: invalid target port %d", port)
	}
	if err := writeFrame(rw, &gwpb.RelayRequest{Host: host, Port: int32(port)}); err != nil {
		return fmt.Errorf("relaywire: write request: %w", err)
	}
	resp := &gwpb.RelayResponse{}
	if err := readFrame(rw, resp); err != nil {
		return fmt.Errorf("relaywire: read response: %w", err)
	}
	// ErrorProto returns nil for codes.OK.
	return gstatus.ErrorProto(resp.GetStatus())
}

// ReadRequest performs the server half of the handshake and returns the
// requested target.
func ReadRequest(r io.Reader) (*gwpb.RelayRequest, error) {
	req := &gwpb.RelayRequest{}
	if err := readFrame(r, req); err != nil {
		return nil, err
	}
	if req.GetHost() == "" {
		return nil, fmt.Errorf("request has no target host")
	}
	if p := req.GetPort(); p <= 0 || p > 0xFFFF {
		return nil, fmt.Errorf("request has invalid target port %d", p)
	}
	return req, nil
}

// Accept tells the client the connection to the target is open and the stream
// now carries its bytes. resolvedAddress is the host:port the target name
// resolved to at the gateway.
func Accept(w io.Writer, resolvedAddress string) error {
	return writeFrame(w, &gwpb.RelayResponse{
		Status:          &spb.Status{Code: int32(codes.OK)},
		ResolvedAddress: resolvedAddress,
	})
}

// Refuse answers the client with the code and message of err, which should be
// a status error (see server/util/status) explaining the refusal in terms the
// developer at the far end can act on. An error without a status code is sent
// as codes.Unknown.
func Refuse(w io.Writer, err error) {
	_ = writeFrame(w, &gwpb.RelayResponse{
		Status: gstatus.Convert(err).Proto(),
	})
}

func writeFrame(w io.Writer, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	frame := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(frame, uint32(len(b)))
	copy(frame[4:], b)
	_, err = w.Write(frame)
	return err
}

func readFrame(r io.Reader, m proto.Message) error {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return fmt.Errorf("read length prefix: %w", err)
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > maxFrameSize {
		return fmt.Errorf("frame of %d bytes exceeds the %d byte limit (is the other side speaking this protocol?)", n, maxFrameSize)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return fmt.Errorf("read frame body: %w", err)
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("unmarshal frame: %w", err)
	}
	return nil
}
