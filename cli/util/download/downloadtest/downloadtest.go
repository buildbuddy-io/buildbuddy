package downloadtest

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
)

type FakeDownloader struct {
	files map[string][]byte
	errs  map[string]error
}

var _ download.Downloader = (*FakeDownloader)(nil)

func New() *FakeDownloader {
	return &FakeDownloader{
		files: map[string][]byte{},
		errs:  map[string]error{},
	}
}

// Add registers content to be returned for uri. It returns the receiver to
// allow chaining.
func (f *FakeDownloader) Add(uri string, content []byte) *FakeDownloader {
	f.files[uri] = content
	return f
}

// AddErr registers an error to be returned for uri instead of content. It
// returns the receiver to allow chaining.
func (f *FakeDownloader) AddErr(uri string, err error) *FakeDownloader {
	f.errs[uri] = err
	return f
}

// GetBytestreamFile implements download.Downloader.
func (f *FakeDownloader) GetBytestreamFile(ctx context.Context, uri string, w io.Writer) error {
	if err, ok := f.errs[uri]; ok {
		return err
	}
	content, ok := f.files[uri]
	if !ok {
		return fmt.Errorf("downloadtest: no content registered for uri %q", uri)
	}
	_, err := w.Write(content)
	return err
}
