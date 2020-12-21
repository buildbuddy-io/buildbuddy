package diskproxy

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	downloadPath = "/download/"
	uploadPath   = "/upload/"
)

type DiskProxy struct {
	rootDir      string
	fileServer   *http.Server
	readNotifyFn ReadNotifyFn
	writeFn      WriteFn
}

type WriteFn func(ctx context.Context, filePath string) (io.WriteCloser, error)
type ReadNotifyFn func(filePath string)

func NewDiskProxy(listenAddr, rootDir string, writeFn WriteFn, readNotifyFn ReadNotifyFn) *DiskProxy {
	mux := http.NewServeMux()
	dp := &DiskProxy{
		rootDir:      rootDir,
		writeFn:      writeFn,
		readNotifyFn: readNotifyFn,
	}
	fileHandler := dp.wrapNotify(http.FileServer(http.Dir(rootDir)))
	mux.Handle(downloadPath, http.StripPrefix(downloadPath, fileHandler))
	mux.Handle(uploadPath, http.StripPrefix(uploadPath, dp))
	dp.fileServer = &http.Server{
		Addr:    listenAddr,
		Handler: http.Handler(mux),
	}
	go func() {
		log.Printf("DiskProxy listening on %q", listenAddr)
		dp.fileServer.ListenAndServe()
	}()
	return dp
}

func (c *DiskProxy) wrapNotify(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		readPath := filepath.Join(c.rootDir, r.URL.Path)
		c.readNotifyFn(readPath)
		h.ServeHTTP(w, r)
		log.Printf("Read remote file %q in %s", readPath, time.Since(start))
	})
}

func (c *DiskProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	filePath := r.URL.Path
	writePath := filepath.Join(c.rootDir, filePath)
	stat, err := os.Stat(writePath)
	if err == nil && os.IsExist(err) {
		w.Header().Set("Content-Length", strconv.FormatInt(stat.Size(), 10))
		w.WriteHeader(200)
		return
	}
	writeCloser, err := c.writeFn(ctx, writePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer writeCloser.Close()
	n, err := io.Copy(writeCloser, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if n != r.ContentLength {
		errMsg := fmt.Sprintf("Content length mismatch! (content-length %d, wrote %d)", r.ContentLength, n)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	log.Printf("Wrote remote file %q in %s", writePath, time.Since(start))
}

func (c *DiskProxy) remoteFileURL(peer, k string, upload bool) string {
	if !strings.HasPrefix(peer, "http") {
		peer = "http://" + peer
	}
	u, _ := url.Parse(peer)
	remoteFilePath := strings.TrimPrefix(k, c.rootDir)
	actionPath := downloadPath
	if upload {
		actionPath = uploadPath
	}
	u.Path += path.Join(actionPath, remoteFilePath)
	return u.String()
}

func (c *DiskProxy) uploadURL(peer, k string) string {
	return c.remoteFileURL(peer, k, true)
}

func (c *DiskProxy) downloadURL(peer, k string) string {
	return c.remoteFileURL(peer, k, false)
}

func (c *DiskProxy) RemoteContains(ctx context.Context, peer, k string) (bool, error) {
	rsp, err := http.Head(c.downloadURL(peer, k))
	if err != nil {
		return false, err
	}
	return rsp.StatusCode == 200, nil
}

func (c *DiskProxy) RemoteReader(ctx context.Context, peer, k string) (io.Reader, error) {
	rsp, err := http.Get(c.downloadURL(peer, k))
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode == 200 {
		return rsp.Body, nil
	} else if rsp.StatusCode == 404 {
		return nil, status.NotFoundError("File not found (remotely).")
	} else {
		return nil, status.InternalErrorf("Unexpected error: %+v, error: %s", rsp, err)
	}
}

func (c *DiskProxy) RemoteGet(ctx context.Context, peer, k string) ([]byte, error) {
	reader, err := c.RemoteReader(ctx, peer, k)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *DiskProxy) RemoteWriter(ctx context.Context, peer, k string, sizeBytes int64) (io.WriteCloser, error) {
	reader, writer := io.Pipe()
	req, _ := http.NewRequest("POST", c.uploadURL(peer, k), reader)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = sizeBytes
	go func() {
		client := &http.Client{}
		_, err := client.Do(req)
		if err != nil {
			reader.CloseWithError(err)
		}
	}()
	return writer, nil
}

func (c *DiskProxy) RemoteSet(ctx context.Context, peer, k string, data []byte) error {
	w, err := c.RemoteWriter(ctx, peer, k, int64(len(data)))
	if err != nil {
		return err
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		log.Printf("Requested write of size %d, only wrote %d", len(data), n)
	}
	return w.Close()
}
