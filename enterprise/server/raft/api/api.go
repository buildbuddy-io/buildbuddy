package api

import (
	"context"
	"io"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dragonboat "github.com/lni/dragonboat/v3"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
)

const readBufSizeBytes = 1000000 // 1MB

type Server struct {
	fileDir string
	nodeHost *dragonboat.NodeHost
	createStateMachineFn   dbsm.CreateOnDiskStateMachineFunc
}

func NewServer(fileDir string, nodeHost *dragonboat.NodeHost, createStateMachineFn dbsm.CreateOnDiskStateMachineFunc) (*Server, error) {
	s := &Server{
		fileDir: fileDir,
		nodeHost: nodeHost,
		createStateMachineFn: createStateMachineFn,
	}
	return s, nil
}

func (s *Server) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	log.Warningf("Got req: %+v", req)
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	if err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), false /*=join*/, s.createStateMachineFn, rc); err != nil {
		return nil, err
	}
	return &rfpb.StartClusterResponse{}, nil
}

func (s *Server) filepath(r *rfpb.FileRecord) (string, error) {
	// This function cannot change without a data migration.
	// filepaths look like this:
	//   // {rootDir}/{groupID}/{ac|cas}/{hashPrefix:4}/{hash}
	//   // for example:
	//   //   /bb/files/GR123456/ac/abcd/abcd12345asdasdasd123123123asdasdasd
	segments := make([]string, 0, 5)
	segments = append(segments, s.fileDir)

	if r.GetGroupId() == "" {
		return "", status.FailedPreconditionError("Empty group ID not allowed in filerecord.")
	}
	segments = append(segments, r.GetGroupId())

	if r.GetIsolation().GetCacheType() == rfpb.Isolation_CAS_CACHE {
		segments = append(segments, "cas")
	} else if r.GetIsolation().GetCacheType() == rfpb.Isolation_ACTION_CACHE {
		segments = append(segments, "ac")
	} else {
		return "", status.FailedPreconditionError("Isolation type must be explicitly set, not UNKNOWN.")
	}
	if len(r.GetDigest().GetHash()) > 4 {
		segments = append(segments, r.GetDigest().GetHash()[:4])
	} else {
		return "", status.FailedPreconditionError("Malformed digest; too short.")
	}
	segments = append(segments, r.GetDigest().GetHash())
	return filepath.Join(segments...), nil
}

type streamWriter struct {
	stream rfspb.Api_ReadServer
}

func (w *streamWriter) Write(buf []byte) (int, error) {
	err := w.stream.Send(&rfpb.ReadResponse{
		Data: buf,
	})
	return len(buf), err
}

func (s *Server) Read(req *rfpb.ReadRequest, stream rfspb.Api_ReadServer) error {
	file, err := s.filepath(req.GetFileRecord())
	if err != nil {
		return err
	}
	reader, err := disk.FileReader(stream.Context(), file, req.GetOffset(), 0)
	if err != nil {
		return err
	}
	defer reader.Close()

	bufSize := int64(readBufSizeBytes)
	d := req.GetFileRecord().GetDigest()
	if d.GetSizeBytes() > 0 && d.GetSizeBytes() < bufSize {
		bufSize = d.GetSizeBytes()
	}
	copyBuf := make([]byte, bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, reader, copyBuf)
	log.Debugf("Read(%q) succeeded.", file)
	return err
}

func (s *Server) Write(stream rfspb.Api_WriteServer) error {
	var bytesWritten int64
	var writeCloser io.WriteCloser
	var file string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			file, err = s.filepath(req.GetFileRecord())
			if err != nil {
				return err
			}
			wc, err := disk.FileWriter(stream.Context(), file)
			if err != nil {
				return err
			}
			writeCloser = wc
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			if err := writeCloser.Close(); err != nil {
				return err
			}
			log.Debugf("Write(%q) succeeded.", file)
			return stream.SendAndClose(&rfpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}
