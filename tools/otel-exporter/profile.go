package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"sort"
	"strings"
	"time"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func (d *daemon) processBazelProfile(ctx context.Context, profile *bespb.File, startTime time.Time) error {
	rn, err := getResourceNameFromURI(profile.GetUri())
	if err != nil {
		return err
	}

	profileRaw, err := d.downloadProfileBlob(ctx, rn, strings.HasSuffix(profile.GetName(), ".gz"))
	if err != nil {
		return err
	}

	var bProfile BazelProfile
	if err := json.Unmarshal(profileRaw, &bProfile); err != nil {
		// Assuming the JSON is malformed due to unexpected crash.
		// Attempt to unmarshal a second time.
		if err2 := json.Unmarshal(append(profileRaw, []byte("]}")...), &bProfile); err2 != nil {
			err := fmt.Errorf("error parsing profile first time: %v\n", err)
			return fmt.Errorf("error parsing profile second time: %v\n%v\n", err, err2)
		}
	}

	var offset time.Duration
	var buildPhases []TraceEvent
	for _, event := range bProfile.TraceEvents {
		if event.Phase == TypeInstant {
			buildPhases = append(buildPhases, event)
			continue
		}
		if event.Phase == TypeComplete && event.Name == "Launch Blaze" {
			// we are using timestamp from BES instead of profile time
			// so we want to make sure to account for the offset in Launch Blaze time
			//
			// Related: https://github.com/bazelbuild/bazel/pull/17636
			offset = time.Microsecond * time.Duration(event.Duration)

			buildPhases = append(buildPhases, event)
			continue
		}
	}
	sort.Slice(buildPhases, func(i, j int) bool {
		return buildPhases[i].TimeStamp < buildPhases[j].TimeStamp
	})

	for i := 0; i < len(buildPhases)-2; i++ {
		start := startTime.Add(time.Microsecond*time.Duration(buildPhases[i].TimeStamp) + offset)
		end := startTime.Add(time.Microsecond*time.Duration(buildPhases[i+1].TimeStamp) + offset)

		_, span := d.tracer.Start(ctx, buildPhases[i].Name, trace.WithTimestamp(start))
		span.End(trace.WithTimestamp(end))
	}

	return nil
}

func (d *daemon) downloadProfileBlob(ctx context.Context, rn *digest.ResourceName, isCompressed bool) ([]byte, error) {
	var out bytes.Buffer
	outCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *bbAPIKey)
	if err := cachetools.GetBlob(outCtx, d.byteStreamClient, rn, &out); err != nil {
		return nil, err
	}

	var reader io.Reader = &out
	if isCompressed {
		gzReader, err := gzip.NewReader(&out)
		defer gzReader.Close()
		if err != nil {
			return nil, fmt.Errorf("error decompressing profile: %v", err)
		}

		reader = gzReader
	}
	profileRaw, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading profile: %v", err)
	}

	return profileRaw, nil
}

func getResourceNameFromURI(uri string) (*digest.ResourceName, error) {
	// should be in the form of
	//   bytestream://remote.buildbuddy.io/blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/99999
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing profile uri: %s", uri)
	}

	return digest.ParseDownloadResourceName(strings.TrimPrefix(u.RequestURI(), "/"))
}
