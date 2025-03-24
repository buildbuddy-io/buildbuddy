package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func computeSHA256(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func ServeContentWithETagCaching(rs io.ReadSeeker) http.Handler {
	var precomputedHash string
	if br, ok := rs.(*bytes.Reader); ok {
		br.Seek(0, io.SeekStart)
		precomputedHash, _ = computeSHA256(br)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := rs.Seek(0, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		hash := precomputedHash
		if hash == "" {
			h, err := computeSHA256(rs)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			hash = h
		}
		etag := `"` + hash + `"`
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if _, err := rs.Seek(0, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		io.Copy(w, rs)
	})
}

func SetContentType(h http.Handler, contentType string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		h.ServeHTTP(w, r)
	})
}

type logErrorsWriter struct {
	http.ResponseWriter
	status              int
	logNextWriteAsError bool
}

func (w *logErrorsWriter) WriteHeader(status int) {
	w.status = status
	if status >= 500 {
		w.logNextWriteAsError = true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *logErrorsWriter) Write(b []byte) (int, error) {
	// To avoid buffering, assume only one Write will occur.
	if w.logNextWriteAsError {
		w.logNextWriteAsError = false
		log.Printf("Error (HTTP %d): %s", w.status, strings.TrimSpace(string(b)))
	}
	// if w.status >= 500 {
	// 	// Make internal server errors opaque.
	// 	b = []byte("internal server error")
	// }
	return w.ResponseWriter.Write(b)
}

func LogServerErrors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(&logErrorsWriter{ResponseWriter: w}, r)
	})
}
