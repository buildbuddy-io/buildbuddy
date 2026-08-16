// Package git_trace2 provides utilities for processing Git Trace2 events in
// real time.
package git_trace2

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const (
	// maxEventSize limits the memory allocated for a single newline-delimited
	// Trace2 JSON event.
	maxEventSize = 1024 * 1024 // 1 MiB

	// drainTimeout limits how long Stop waits for lingering Trace2 writers,
	// such as detached background maintenance processes that inherited the
	// pipe and outlived the Git command.
	drainTimeout = time.Second
)

// Event is a decoded Git Trace2 event.
type Event struct {
	// Type identifies the Trace2 event type, such as data or region_enter.
	Type string `json:"event"`
	// SessionID identifies the Git process and its ancestry.
	SessionID string `json:"sid"`
	// Category groups related data and region events.
	Category string `json:"category"`
	// Label identifies a Trace2 region.
	Label string `json:"label"`
	// Key identifies a Trace2 data value.
	Key string `json:"key"`
	// Value contains the raw JSON value of a Trace2 data value: a JSON
	// string for data events, or an arbitrary JSON value for data_json
	// events.
	Value json.RawMessage `json:"value"`
}

// StringValue returns the event's value decoded as a JSON string, or an empty
// string if the value is absent or not a string (e.g. for data_json events).
func (e *Event) StringValue() string {
	var s string
	if err := json.Unmarshal(e.Value, &s); err != nil {
		return ""
	}
	return s
}

// Handler processes a decoded Trace2 event. Events are delivered from a
// single goroutine in the order they are received. Handlers must not block
// (Stop waits for the event stream to drain) and must not panic (a panic
// unwinds the processing goroutine and crashes the process).
type Handler func(*Event)

// Processor reads Trace2 events that a Git process tree writes to a shared
// pipe.
type Processor struct {
	r        *os.File
	w        *os.File
	handler  Handler
	done     chan struct{}
	stopped  atomic.Bool
	stopOnce sync.Once
}

// StartProcessor starts a processor that calls handler for each valid Trace2
// event written by attached Git commands.
func StartProcessor(handler Handler) (*Processor, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create trace2 pipe: %w", err)
	}
	p := &Processor{r: r, w: w, handler: handler, done: make(chan struct{})}
	go p.process()
	return p, nil
}

// Attach configures a Git command to stream Trace2 events to the processor.
// The command and its descendant processes (including submodule fetches)
// inherit the pipe and write their events to it. Attach must be called
// before the command starts and before Stop; it may be called on several
// commands in turn.
func (p *Processor) Attach(cmd *exec.Cmd) error {
	if p.stopped.Load() {
		return errors.New("processor is stopped")
	}
	// ExtraFiles[i] becomes descriptor 3+i in the child.
	fd := 3 + len(cmd.ExtraFiles)
	if fd > 9 {
		return fmt.Errorf("descriptor %d exceeds git's single-digit trace2 target limit", fd)
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, p.w)
	cmd.Env = append(cmd.Environ(),
		"GIT_TRACE2_EVENT="+strconv.Itoa(fd),
		// Data events such as fetch progress can be nested inside other
		// trace2 regions; raise the nesting limit (default 2) so they aren't
		// dropped.
		"GIT_TRACE2_EVENT_NESTING=5",
	)
	return nil
}

func (p *Processor) process() {
	defer close(p.done)
	scanner := bufio.NewScanner(p.r)
	scanner.Buffer(nil, maxEventSize)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			log.Debugf("Ignoring unparseable Trace2 event: %s", err)
			continue
		}
		p.handler(&event)
	}
	if err := scanner.Err(); err != nil {
		log.Warningf("Git Trace2 event stream ended early: %s", err)
		// Keep consuming the pipe so that attached Git processes are never
		// blocked writing trace2 events to a full pipe.
		_, _ = io.Copy(io.Discard, p.r)
	}
}

// Stop drains the remaining Trace2 events and releases the pipe. Call it
// after the Git command exits; it is safe to call more than once.
func (p *Processor) Stop() {
	p.stopped.Store(true)
	p.stopOnce.Do(func() {
		// Close this process's copy of the write end so that the processing
		// goroutine sees EOF once every Git process has exited.
		_ = p.w.Close()
		// Time out reads so that a lingering writer cannot block shutdown
		// indefinitely.
		_ = p.r.SetReadDeadline(time.Now().Add(drainTimeout))
		<-p.done
		_ = p.r.Close()
	})
}
