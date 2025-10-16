package main

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func ServeBytesWithETagCaching(b []byte) http.Handler {
	etag := `"` + fmt.Sprintf("%x", sha256.Sum256(b)) + `"`
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write(b)
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
