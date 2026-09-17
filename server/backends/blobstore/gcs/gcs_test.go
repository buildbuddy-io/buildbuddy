package gcs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	storageapi "google.golang.org/api/storage/v1"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestArchiveLifecycle(t *testing.T) {
	const rule = `{"rule":[{"action":{"type":"SetStorageClass","storageClass":"ARCHIVE"},"condition":{"age":7,"isLive":true,"matchesStorageClass":["STANDARD"],"sizeAboveBytes":"2097152"}}]}`
	for _, tc := range []struct {
		name        string
		attrs       string
		ageDays     int64
		sizeAbove   int64
		patchStatus int
		wantPatch   bool
		wantError   bool
	}{
		{name: "install", attrs: `{"metageneration":"42"}`, ageDays: 7, sizeAbove: 2097152, wantPatch: true},
		{name: "already configured", attrs: `{"metageneration":"42","lifecycle":` + rule + `}`, ageDays: 7, sizeAbove: 2097152},
		{name: "change size", attrs: `{"metageneration":"42","lifecycle":` + rule + `}`, ageDays: 7, sizeAbove: 1048576, wantPatch: true},
		{name: "one byte exclusive bound", attrs: `{"metageneration":"42"}`, ageDays: 7, sizeAbove: 1, wantPatch: true},
		{name: "autoclass", attrs: `{"metageneration":"42","autoclass":{"enabled":true}}`, ageDays: 7, sizeAbove: 2097152, wantError: true},
		{name: "permission denied", attrs: `{"metageneration":"42"}`, ageDays: 7, sizeAbove: 2097152, patchStatus: 403, wantPatch: true, wantError: true},
		{name: "change days", attrs: `{"lifecycle":` + rule + `}`, ageDays: 30, sizeAbove: 2097152, wantPatch: true},
		{name: "change class", attrs: `{"lifecycle":` + strings.ReplaceAll(rule, "ARCHIVE", "COLDLINE") + `}`, ageDays: 7, sizeAbove: 2097152, wantPatch: true},
		{name: "change action", attrs: `{"lifecycle":` + strings.ReplaceAll(rule, "SetStorageClass", "Delete") + `}`, ageDays: 7, sizeAbove: 2097152, wantPatch: true},
		{name: "matching rule with extra condition", attrs: `{"lifecycle":` + strings.ReplaceAll(rule, `"isLive":true`, `"isLive":true,"matchesPrefix":["example/"]`) + `}`, ageDays: 7, sizeAbove: 2097152},
		{name: "matching rule with extra rules", attrs: `{"lifecycle":` + strings.Replace(rule, `"rule":[`, `"rule":[{"action":{"type":"Delete"},"condition":{"age":365}},`, 1) + `}`, ageDays: 7, sizeAbove: 2097152},
		{name: "disabled preserves rules", attrs: `{"lifecycle":` + rule + `}`, ageDays: 0},
		{name: "disabled with empty rules", attrs: `{"lifecycle":{"rule":[]}}`, ageDays: 0},
		{name: "disabled with no lifecycle", attrs: `{}`, ageDays: 0},
		{name: "disabled with autoclass", attrs: `{"autoclass":{"enabled":true},"lifecycle":` + rule + `}`, ageDays: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patched := false
			hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if tc.ageDays == 0 {
					t.Fatal("disabled reconciliation must not make API requests")
				}
				body, code := tc.attrs, http.StatusOK
				if r.Method == http.MethodPatch {
					patched = true
					if got := r.URL.Query().Get("ifMetagenerationMatch"); got != "" {
						t.Fatalf("unexpected metageneration precondition: %q", got)
					}
					var payload map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					if len(payload) != 1 || payload["lifecycle"] == nil {
						t.Fatalf("patch must change only lifecycle: %v", payload)
					}
					var lc storageapi.BucketLifecycle
					if err := json.Unmarshal(payload["lifecycle"], &lc); err != nil {
						t.Fatal(err)
					}
					if len(lc.Rule) != 1 || lc.Rule[0].Action.Type != "SetStorageClass" || lc.Rule[0].Action.StorageClass != "ARCHIVE" {
						t.Fatalf("unexpected lifecycle: %s", payload["lifecycle"])
					}
					c := lc.Rule[0].Condition
					if c == nil || c.Age == nil || *c.Age != tc.ageDays || c.IsLive == nil || !*c.IsLive || len(c.MatchesStorageClass) != 1 || c.MatchesStorageClass[0] != "STANDARD" || c.SizeAboveBytes != tc.sizeAbove {
						t.Fatalf("unexpected lifecycle condition: %+v", c)
					}
					body = `{}`
					if tc.patchStatus != 0 {
						code = tc.patchStatus
						body = `{"error":{"code":403,"message":"permission denied"}}`
					}
				} else if r.Method != http.MethodGet {
					t.Fatalf("unexpected method: %s", r.Method)
				}
				return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			opts := []option.ClientOption{option.WithHTTPClient(hc)}
			client, err := storage.NewClient(context.Background(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			g := &GCSBlobStore{clientOptions: opts, bucketHandle: client.Bucket("test-bucket")}
			err = g.setBucketArchiveLifecycle(context.Background(), tc.ageDays, tc.sizeAbove)
			if (err != nil) != tc.wantError || patched != tc.wantPatch {
				t.Fatalf("error = %v, patched = %v; want error = %v, patched = %v", err, patched, tc.wantError, tc.wantPatch)
			}
		})
	}
}
