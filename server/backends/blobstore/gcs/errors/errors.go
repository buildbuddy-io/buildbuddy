package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/grpc/status"
)

const (
	bucketName  = "vanja_test_blobs"
	objectName  = "test-object.txt"
	concurrency = 10
	totalWrites = 1000
)

func main() {
	ctx := context.Background()
	client, err := storage.NewGRPCClient(ctx)
	// client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create storage client: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup

	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < totalWrites; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // limit concurrent writes

		go func(i int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			obj := client.Bucket(bucketName).Object(objectName).If(storage.Conditions{DoesNotExist: true})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			w := obj.NewWriter(ctx)

			// Add some unique content
			data := fmt.Sprintf("Writer #%d: timestamp %v\n", i, time.Now())
			if _, err := io.WriteString(w, data); err != nil {
				var apierr *apierror.APIError
				if errors.As(err, &apierr) {
					log.Printf("API error on write #%d: %v", i, apierr)
				} else {
					log.Printf("Other error on write #%d: %v", i, err)
				}
				log.Printf("write #%d failed: %T: %v", i, err, err)
				_ = w.Close()
				return
			}

			// Attempt to close the writer
			if err := w.Close(); err != nil {
				var apierr *apierror.APIError
				if errors.As(err, &apierr) {
					log.Printf("********* API error on close #%d: %T; %v", i, apierr.Unwrap(), apierr)
					log.Printf("********* %#v", apierr)
					log.Printf("********* %#v", apierr.HTTPCode())
					log.Printf("********* %v", apierr.GRPCStatus())
				} else if s, ok := status.FromError(err); ok {
					log.Printf("********* gRPC error on close #%d: %v", i, s.Message())
				} else {
					log.Printf("********* Other error on close #%d: %v", i, err)
				}
				// log.Printf("********* writer #%d close failed: %T: %#v", i, err, err)
				ae, ok := apierror.FromError(err)
				log.Printf("********* writer #%d close failed (%v): %#v", i, ok, ae)
			}
		}(i)
	}

	wg.Wait()

}
