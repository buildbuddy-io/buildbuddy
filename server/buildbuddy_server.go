package buildbuddy_server

import (
	"context"
	"io/ioutil"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/tryflame/buildbuddy/server/build_event_handler"
	inpb "proto/invocation"
)

type BuildBuddyServer struct {
	eventHandler *build_event_handler.BuildEventHandler
}

func NewBuildBuddyServer(h *build_event_handler.BuildEventHandler) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		eventHandler: h,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	inv, err := s.eventHandler.LookupInvocation(ctx, req.Query.InvocationId)
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}, nil
}

func (s *BuildBuddyServer) GetInvocationHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		req := inpb.GetInvocationRequest{}
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rsp, err := s.GetInvocation(context.Background(), &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err := proto.Marshal(rsp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/protobuf")
		w.Write(data)
	})
}
