package buildbuddy_server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	inpb "proto/invocation"
	uspb "proto/user"
)

type BuildBuddyServer struct {
	env environment.Env
}

func NewBuildBuddyServer(env environment.Env) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		env: env,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	if req.GetLookup().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("GetInvocationRequest must contain a valid invocation_id")
	}
	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}, nil
}

func (s *BuildBuddyServer) SearchInvocation(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentErrorf("SearchInvocationRequest cannot be empty")
	}
	searcher := s.env.GetSearcher()
	if searcher == nil {
		return nil, fmt.Errorf("No searcher was configured")
	}
	if req.Query == nil {
		return nil, fmt.Errorf("A query must be provided")
	}
	return searcher.QueryInvocations(ctx, req)
}

func (s *BuildBuddyServer) GetUser(ctx context.Context, req *uspb.GetUserRequest) (*uspb.GetUserResponse, error) {
	return nil, status.UnimplementedError("Not Implemented")
}

func (s *BuildBuddyServer) ModifyUser(ctx context.Context, req *uspb.ModifyUserRequest) (*uspb.ModifyUserResponse, error) {
	return nil, status.UnimplementedError("Not Implemented")
}

type bsLookup struct {
	HostPort string
	Blob     string
	Filename string
}

func filenameFromBlobname(blobname string) string {
	parts := strings.Split(blobname, "/")
	if len(parts) == 3 {
		return parts[1]
	}
	return blobname
}

func parseFilename(filename string) string {
	parts := strings.Split(filename, "/")
	return parts[len(parts)-1]
}

func parseByteStreamURL(bsURL, filename string) (*bsLookup, error) {
	bsPrefix := "bytestream://"
	if strings.HasPrefix(bsURL, bsPrefix) {
		hostBlobString := strings.TrimPrefix(bsURL, bsPrefix)
		parts := strings.SplitN(hostBlobString, "/", 2)
		if len(parts) == 2 {
			bsl := &bsLookup{
				HostPort: parts[0],
				Blob:     parts[1],
				Filename: parseFilename(filename),
			}
			if bsl.Filename == "" {
				bsl.Filename = filenameFromBlobname(bsl.Blob)
			}
			return bsl, nil
		}
	}
	return nil, fmt.Errorf("unparsable bytestream URL: '%s'", bsURL)
}

// Handle requests for build logs and artifacts by looking them up in from our
// cache server using the bytestream API.
func (s *BuildBuddyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	lookup, err := parseByteStreamURL(params.Get("bytestream_url"), params.Get("filename"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Connect to host/port and create a new client
	conn, err := grpc.Dial(lookup.HostPort, grpc.WithInsecure())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer conn.Close()
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: lookup.Blob,
		ReadOffset:   0, // started from the bottom now we here
		ReadLimit:    0, // no limit
	}
	stream, err := client.Read(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Stream the file back to our client
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", lookup.Filename))
	w.Header().Set("Content-Type", "application/octet-stream")
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(rsp.Data)
	}
}
