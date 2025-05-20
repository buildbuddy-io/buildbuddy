package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"
	"runtime/trace"
	
	etrace "golang.org/x/exp/trace"
	"golang.org/x/sync/errgroup"
)

// hotLoop burns CPU until ctx is canceled.
func hotLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Done hotlooping!")
			return
		default:
			continue
		}
	}
}

// hotLoopWithLabel runs hotLoop and applies a trace task label.
func hotLoopWithLabel(ctx context.Context, label string) {
	ctx, task := trace.NewTask(ctx, label)
	trace.Log(ctx, "hotloop-label", label)
	defer task.End()
	hotLoop(ctx)
}

// printStats dumps stats from the flight recorder
func printStats(ctx context.Context, fr *etrace.FlightRecorder) {
	var b bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// exit
		}
		b.Reset()
		if _, err := fr.WriteTo(&b); err != nil {
			log.Fatal(err)
		}
		r, err := etrace.NewReader(&b)
		if err != nil {
			log.Fatal(err)
		}
		for {
			// Read the event.
			ev, err := r.ReadEvent()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			log.Printf("event: %q", ev.String())
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	fr := etrace.NewFlightRecorder()
	fr.Start()
	
	eg, gctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		printStats(gctx, fr)
		return nil
	})
	
	for i := 1; i < 5; i++ {
		d := time.Duration(i) * time.Second
		name := fmt.Sprintf("g-%d", i)
		eg.Go(func() error {
			tctx, cancel := context.WithTimeout(gctx, d)
			defer cancel()
			hotLoopWithLabel(tctx, name) 
			return nil
		})
		log.Printf("started hotloop %s dur: %s", name, d)
	}
	eg.Wait()
}
