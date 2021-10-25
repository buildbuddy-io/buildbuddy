package api

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dragonboat "github.com/lni/dragonboat/v3"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const readBufSizeBytes = 1000000 // 1MB

type Server struct {
	fileDir              string
	nodeHost             *dragonboat.NodeHost
	createStateMachineFn dbsm.CreateOnDiskStateMachineFunc
}

func NewServer(fileDir string, nodeHost *dragonboat.NodeHost, createStateMachineFn dbsm.CreateOnDiskStateMachineFunc) (*Server, error) {
	s := &Server{
		fileDir:              fileDir,
		nodeHost:             nodeHost,
		createStateMachineFn: createStateMachineFn,
	}
	return s, nil
}

func (s *Server) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	if err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), false /*=join*/, s.createStateMachineFn, rc); err != nil {
		return nil, err
	}
	return &rfpb.StartClusterResponse{}, nil
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
	file, err := constants.FilePath(s.fileDir, req.GetFileRecord())
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
			file, err = constants.FilePath(s.fileDir, req.GetFileRecord())
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

func (s *Server) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	sesh := s.nodeHost.GetNoOPSession(req.GetReplica().GetClusterId())
	buf, err := proto.Marshal(req.GetRequest())
	if err != nil {
		return nil, err
	}
	rrsp, err := s.nodeHost.SyncPropose(ctx, sesh, buf)
	if err != nil {
		return nil, err
	}
	ru := &rfpb.ResponseUnion{}
	if err := proto.Unmarshal(rrsp.Data, ru); err != nil {
		return nil, err
	}
	return &rfpb.SyncProposeResponse{
		Response: ru,
	}, nil
}

func (s *Server) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	buf, err := proto.Marshal(req.GetRequest())
	if err != nil {
		return nil, err
	}
	rrsp, err := s.nodeHost.SyncRead(ctx, req.GetReplica().GetClusterId(), buf)
	if err != nil {
		return nil, err
	}
	ru := &rfpb.ResponseUnion{}
	if buf, ok := rrsp.([]byte); ok {
		if err := proto.Unmarshal(buf, ru); err != nil {
			return nil, err
		}
	}
	return &rfpb.SyncReadResponse{
		Response: ru,
	}, nil
}
