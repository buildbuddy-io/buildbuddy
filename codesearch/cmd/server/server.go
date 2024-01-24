package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/regexp"
	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch/codesearch_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/codesearch/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/codesearch/search"
)

var (
	listen   = flag.String("listen", ":2633", "Address and port to listen on")
	indexDir = flag.String("index_dir", "", "Directory to store index in. Default: '~/.csindex/'")
)

type codesearchServer struct {
	db *pebble.DB
}

func defaultDir() string {
	var home string
	home = os.Getenv("HOME")
	if runtime.GOOS == "windows" && home == "" {
		home = os.Getenv("USERPROFILE")
	}
	return filepath.Clean(home + "/.csindex")
}

func New() (*codesearchServer, error) {
	d := *indexDir
	if d == "" {
		d = defaultDir()
	}
	db, err := pebble.Open(d, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &codesearchServer{
		db: db,
	}, nil
}

func (css *codesearchServer) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	log.Printf("Index RPC")
	iw, err := index.Create(css.db)
	if err != nil {
		return nil, err
	}
	_ = iw
	return &inpb.IndexResponse{}, nil
}

func (css *codesearchServer) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	log.Printf("Search RPC")
	ir := index.Open(css.db)

	g := regexp.Grep{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}

	pat := "(?m)" + req.GetQuery().GetTerm()
	pat = "(?i)" + pat

	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, err
	}
	g.Regexp = re

	q := query.RegexpQuery(re.Syntax)
	log.Printf("query: %s\n", q)

	matchingFiles, err := ir.PostingQuery(q)
	if err != nil {
		return nil, err
	}

	rsp := &srpb.SearchResponse{}
	for _, fileid := range matchingFiles {
		name, err := ir.Name(fileid)
		if err != nil {
			return nil, err
		}
		buf, err := ir.Contents(fileid)
		if err != nil {
			return nil, err
		}
		result, err := g.MakeResult(bytes.NewReader(buf), name)
		if err != nil {
			return nil, err
		}
		rsp.Results = append(rsp.Results, result.ToProto())
	}

	return rsp, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	css, err := New()
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	csspb.RegisterCodesearchServiceServer(grpcServer, css)

	log.Printf("Codesearch server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
