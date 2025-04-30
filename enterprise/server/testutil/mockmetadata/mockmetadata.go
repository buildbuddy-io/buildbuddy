package mockmetadata

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

type Server struct {
	fs  filestore.Store
	lru *lru.LRU[*sgpb.FileMetadata]
}

func NewServer(maxSizeBytes int64) (*Server, error) {
	l, err := lru.NewLRU[*sgpb.FileMetadata](&lru.Config[*sgpb.FileMetadata]{
		MaxSize: maxSizeBytes,
		OnEvict: func(key string, value *sgpb.FileMetadata, reason lru.EvictionReason) {
			log.Infof("Evicted %+v (REASON: %s)", value, reason)
		},
		SizeFn: func(value *sgpb.FileMetadata) int64 { return int64(proto.Size(value)) },
	})
	if err != nil {
		return nil, err
	}
	return &Server{
		fs:  filestore.New(),
		lru: l,
	}, nil

}
func (rc *Server) key(r *sgpb.FileRecord) (string, error) {
	pmk, err := rc.fs.PebbleKey(r)
	if err != nil {
		return "", err
	}
	buf, err := pmk.Bytes(filestore.Version5)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (rc *Server) Get(ctx context.Context, req *mdpb.GetRequest) (*mdpb.GetResponse, error) {
	rsp := &mdpb.GetResponse{
		FileMetadatas: make([]*sgpb.FileMetadata, len(req.GetFileRecords())),
	}
	for i, fileRecord := range req.GetFileRecords() {
		key, err := rc.key(fileRecord)
		if err != nil {
			return nil, status.InternalErrorf("error making key: %s", err)
		}
		fm, _ := rc.lru.Get(key)
		rsp.FileMetadatas[i] = fm
	}
	return rsp, nil
}

func (rc *Server) Find(ctx context.Context, req *mdpb.FindRequest) (*mdpb.FindResponse, error) {
	rsp := &mdpb.FindResponse{
		FindResponses: make([]*mdpb.FindResponse_FindOperationResponse, len(req.GetFileRecords())),
	}

	for i, fileRecord := range req.GetFileRecords() {
		key, err := rc.key(fileRecord)
		if err != nil {
			return nil, status.InternalErrorf("error making key: %s", err)
		}
		_, ok := rc.lru.Get(key)
		rsp.FindResponses[i] = &mdpb.FindResponse_FindOperationResponse{
			Present: ok,
		}
	}
	return rsp, nil
}

func (rc *Server) Set(ctx context.Context, req *mdpb.SetRequest) (*mdpb.SetResponse, error) {
	for _, setOp := range req.GetSetOperations() {
		key, err := rc.key(setOp.GetFileMetadata().GetFileRecord())
		if err != nil {
			return nil, status.InternalErrorf("error making key: %s", err)
		}
		// ignore whether or not this is an overwrite; this api doesn't care.
		_ = rc.lru.Add(key, setOp.GetFileMetadata())
	}
	return &mdpb.SetResponse{}, nil
}

func (rc *Server) Delete(ctx context.Context, req *mdpb.DeleteRequest) (*mdpb.DeleteResponse, error) {
	for _, delOp := range req.GetDeleteOperations() {
		key, err := rc.key(delOp.GetFileRecord())
		if err != nil {
			return nil, status.InternalErrorf("error making key: %s", err)
		}

		fm, _ := rc.lru.Get(key)
		if delOp.GetMatchAtime() != 0 && delOp.GetMatchAtime() != fm.GetLastAccessUsec() {
			// Skip deletion if atime match was requested but did
			// not match.
			continue
		}
		// ignore whether or not this is an overwrite; this api doesn't care.
		_ = rc.lru.Remove(key)
	}
	return &mdpb.DeleteResponse{}, nil
}
