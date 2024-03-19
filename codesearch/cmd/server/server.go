package main

import (
	"archive/zip"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	osre "regexp"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/regexp"
	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
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

func resolveIndexDir() string {
	d := *indexDir
	if d == "" {
		d = defaultDir()
	}
	return d
}

func New() (*codesearchServer, error) {
	db, err := pebble.Open(resolveIndexDir(), &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &codesearchServer{
		db: db,
	}, nil
}

func (css *codesearchServer) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	log.Printf("Index RPC")
	gitRepo := req.GetGitRepo()
	if gitRepo == nil {
		return nil, fmt.Errorf("Only git-repo is supported")
	}

	// https://github.com/buildbuddy-io/buildbuddy/archive/1d8a3184c996c3d167a281b70a4eeccd5188e5e1.tar.gz
	archiveURL := fmt.Sprintf("https://github.com/%s/archive/%s.zip", gitRepo.GetOwnerRepo(), gitRepo.GetCommitSha())
	log.Printf("archive URL is %q", archiveURL)

	httpRsp, err := http.Get(archiveURL)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	tmpFile, err := os.CreateTemp(resolveIndexDir(), "archive-*.zip")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	if _, err := io.Copy(tmpFile, httpRsp.Body); err != nil {
		return nil, err
	}
	log.Printf("Copied archive to %q", tmpFile.Name())

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	iw, err := index.Create(css.db)
	if err != nil {
		return nil, err
	}

	for _, file := range zipReader.File {
		in, err := file.Open()
		if err != nil {
			return nil, err
		}
		defer in.Close()
		fileCopy, err := os.CreateTemp(resolveIndexDir(), "*.tmp")
		if err != nil {
			return nil, err
		}
		defer os.Remove(fileCopy.Name())
		if _, err := io.Copy(fileCopy, in); err != nil {
			return nil, err
		}
		fileCopy.Seek(0, 0)

		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) == 1 {
			continue
		}
		filePathOnly := filepath.Join(parts[1:]...)
		if err := iw.Add(filePathOnly, fileCopy); err != nil {
			return nil, err
		}
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}
	return &inpb.IndexResponse{
		GitRepo: &inpb.GitRepoResponse{},
	}, nil
}

func (css *codesearchServer) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	log.Printf("Search RPC")
	ir := index.Open(css.db)

	g := regexp.Grep{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}

	term := req.GetQuery().GetTerm()
	// Try to extract filepath stuff before compiling.
	filepathFilterRegex := osre.MustCompile(`filepath:[^\s]+`)
	filepathRegex := ""
	for _, filepathFilter := range filepathFilterRegex.FindAll([]byte(term), -1) {
		filter := string(filepathFilter)
		term = strings.ReplaceAll(term, filter, "")
		filepathRegex = strings.TrimPrefix(filter, "filepath:")
		log.Printf("term is now %q", term)
	}
	var fre *osre.Regexp
	if filepathRegex != "" {
		var err error
		fre, err = osre.Compile(filepathRegex)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("req: %+v", req)
	pat := "(?m)" + term
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
		if fre != nil && !fre.MatchString(name) {
			continue
		}
		buf, err := ir.Contents(fileid)
		if err != nil {
			return nil, err
		}
		result, err := g.MakeResult(bytes.NewReader(buf), name)
		if err != nil {
			return nil, err
		}
		if result.MatchCount == 0 {
			continue
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
