package logstream

import (
	"context"
	"encoding/base64"
	"io"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/keyval"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	lspb "github.com/buildbuddy-io/buildbuddy/proto/logstream"
)

const (
	// Maximum allowed key length in bytes (before encoding).
	maxKeyLen = 2048
)

func storagePathForKey(key string) (string, error) {
	if key == "" {
		return "", status.InvalidArgumentError("missing key")
	}
	if len(key) > maxKeyLen {
		return "", status.InvalidArgumentError("key too long")
	}
	// Use a stable, filesystem-safe encoding to avoid any path traversal.
	enc := base64.RawURLEncoding.EncodeToString([]byte(key))
	return "logstream/" + enc, nil
}

func pubsubChannelForKey(key string) (string, error) {
	if len(key) > maxKeyLen {
		return "", status.InvalidArgumentError("key too long")
	}
	enc := base64.RawURLEncoding.EncodeToString([]byte(key))
	return "logstream/" + enc + "/updates", nil
}

// Server implements the LogStream service.
type Server struct {
	env environment.Env
}

func New(env environment.Env) *Server {
	return &Server{env: env}
}

func (s *Server) Write(stream lspb.LogStream_WriteServer) error {
	ctx := stream.Context()
	bs := s.env.GetBlobstore()
	if bs == nil {
		return status.FailedPreconditionError("blobstore unavailable")
	}
	kv := s.env.GetKeyValStore()
	pubsub := s.env.GetPubSub()
	cs := chunkstore.New(bs, &chunkstore.ChunkstoreOptions{})

	type writer struct {
		cw   *chunkstore.ChunkstoreWriter
		path string
		ch   string
	}

	writers := make(map[string]*writer)
	getWriter := func(key string) (*writer, error) {
		if w, ok := writers[key]; ok {
			return w, nil
		}
		path, err := storagePathForKey(key)
		if err != nil {
			return nil, err
		}
		ch, err := pubsubChannelForKey(key)
		if err != nil {
			return nil, err
		}
		writeHook := func(ctx context.Context, writeRequest *chunkstore.WriteRequest, writeResult *chunkstore.WriteResult, chunk []byte, volatileTail []byte) {
			if kv == nil {
				return
			}
			if writeResult.Close {
				keyval.SetProto(ctx, kv, path, nil)
				return
			}
			chunkID := chunkstore.ChunkIndexAsStringId(writeResult.LastChunkIndex + 1)
			if chunkID == chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
				keyval.SetProto(ctx, kv, path, nil)
				return
			}
			live := &lspb.LiveLogChunk{
				ChunkId: chunkID,
				Buffer:  append(chunk, volatileTail...),
			}
			_ = keyval.SetProto(ctx, kv, path, live)
			if pubsub != nil {
				pubsub.Publish(ctx, ch, "")
			}
		}

		cw := cs.Writer(ctx, path, &chunkstore.ChunkstoreWriterOptions{
			WriteTimeoutDuration: 15 * time.Second,
			NoSplitWrite:         true,
			WriteHook:            writeHook,
		})
		w := &writer{cw: cw, path: path, ch: ch}
		writers[key] = w
		return w, nil
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			// close all writers
			for _, w := range writers {
				_ = w.cw.Close(ctx)
			}
			if err == io.EOF {
				return stream.SendAndClose(&lspb.WriteLogResponse{})
			}
			if status.IsCanceledError(err) {
				return nil
			}
			return err
		}
		if req.GetKey() == "" {
			return status.InvalidArgumentError("missing key")
		}
		if len(req.GetData()) == 0 {
			continue
		}
		w, err := getWriter(req.GetKey())
		if err != nil {
			return err
		}
		if _, err := w.cw.Write(ctx, req.GetData()); err != nil && !status.IsCanceledError(err) {
			return err
		}
	}
}

func (s *Server) GetLogChunk(ctx context.Context, req *lspb.GetLogChunkRequest) (*lspb.GetLogChunkResponse, error) {
	bs := s.env.GetBlobstore()
	if bs == nil {
		return nil, status.FailedPreconditionError("blobstore unavailable")
	}
	kv := s.env.GetKeyValStore()
	cs := chunkstore.New(bs, &chunkstore.ChunkstoreOptions{})

	path, err := storagePathForKey(req.GetKey())
	if err != nil {
		return nil, err
	}

	// Compute last chunk id.
	lastChunkId, err := cs.GetLastChunkId(ctx, path, chunkstore.ChunkIndexAsStringId(math.MaxUint16))
	if err != nil {
		// No chunks exist.
		return &lspb.GetLogChunkResponse{}, nil
	}
	lastChunkIndex, err := chunkstore.ChunkIdAsUint16Index(lastChunkId)
	if err != nil {
		return nil, err
	}

	// Determine start index.
	startIndex := lastChunkIndex
	step := uint16(1)
	boundary := lastChunkIndex
	if req.GetChunkId() != "" {
		startIndex, err = chunkstore.ChunkIdAsUint16Index(req.GetChunkId())
		if err != nil {
			return nil, err
		}
		// Forward reads
		boundary = lastChunkIndex
		step = 1
	} else {
		// Tail read: read backwards.
		boundary = 0
		step = math.MaxUint16
	}

	// Read chunks until we have enough lines or run out.
	var buf []byte
	lineCount := 0
	for chunkIndex := startIndex; chunkIndex != boundary+step; chunkIndex += step {
		chunk, err := cs.ReadChunk(ctx, path, chunkIndex)
		if err != nil {
			break
		}
		if req.GetChunkId() == "" {
			// Prepend when reading backwards.
			buf = append(chunk, buf...)
		} else {
			buf = append(buf, chunk...)
		}
		for _, b := range chunk {
			if b == '\n' {
				lineCount++
			}
		}
		if req.GetMinLines() <= 0 || lineCount >= int(req.GetMinLines()) {
			break
		}
	}

	rsp := &lspb.GetLogChunkResponse{
		Buffer:      buf,
		NextChunkId: chunkstore.ChunkIndexAsStringId(startIndex + 1),
	}
	if startIndex > 0 {
		rsp.PreviousChunkId = chunkstore.ChunkIndexAsStringId(startIndex - 1)
	}
	if startIndex >= lastChunkIndex {
		// Check live chunk cache.
		if kv != nil {
			live := &lspb.LiveLogChunk{}
			if err := keyval.GetProto(ctx, kv, path, live); err == nil {
				if req.GetChunkId() == live.GetChunkId() {
					return &lspb.GetLogChunkResponse{
						Buffer:      live.GetBuffer(),
						NextChunkId: live.GetChunkId(),
						Live:        true,
					}, nil
				}
			}
		}
		// No more chunks.
		rsp.NextChunkId = ""
	}
	return rsp, nil
}

func (s *Server) GetLog(req *lspb.GetLogChunkRequest, stream lspb.LogStream_GetLogServer) error {
	ctx := stream.Context()
	pubsub := s.env.GetPubSub()

	ch, err := pubsubChannelForKey(req.GetKey())
	if err != nil {
		return err
	}

	updated := make(<-chan string)
	if pubsub != nil {
		sub := pubsub.Subscribe(ctx, ch)
		defer sub.Close()
		updated = sub.Chan()
	}

	// Copy request so we can mutate chunk_id.
	r := &lspb.GetLogChunkRequest{
		RequestContext: req.GetRequestContext(),
		Key:            req.GetKey(),
		ChunkId:        req.GetChunkId(),
		MinLines:       req.GetMinLines(),
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-updated:
		case <-time.After(3 * time.Second):
		}
		for {
			rsp, err := s.GetLogChunk(ctx, r)
			if err != nil {
				return err
			}
			if err := stream.Send(rsp); err != nil {
				return err
			}
			if rsp.GetNextChunkId() == "" {
				return nil
			}
			if r.GetChunkId() == rsp.GetNextChunkId() {
				break
			}
			r.ChunkId = rsp.GetNextChunkId()
		}
	}
}
