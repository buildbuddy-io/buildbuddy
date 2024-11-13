// The trace events package provides definitions and utilities for a subset of
// Google's Trace Event Format that is relevant to BuildBuddy.
//
// The format is documented here:
// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
package trace_events

import (
	"encoding/json"
	"fmt"
	"io"
)

// Phase constants
const (
	PhaseComplete = "X"
	PhaseCounter  = "C"
)

// Profile represents a trace profile, including all trace events.
type Profile struct {
	TraceEvents []*Event `json:"traceEvents,omitempty"`
}

// Event represents a trace event.
type Event struct {
	Category  string         `json:"cat,omitempty"`
	Name      string         `json:"name,omitempty"`
	Phase     string         `json:"ph,omitempty"`
	Timestamp int64          `json:"ts"`
	Duration  int64          `json:"dur"`
	ProcessID int64          `json:"pid,omitempty"`
	ThreadID  int64          `json:"tid,omitempty"`
	Args      map[string]any `json:"args,omitempty"`
}

type eventWriter struct {
	w          io.Writer
	wroteFirst bool
}

// NewEventWriter writes a list of TraceEvents as comma-separated JSON objects.
// It does not write the start or end delimiters of the list.
// Each object is written on its own line.
func NewEventWriter(w io.Writer) *eventWriter {
	return &eventWriter{w: w}
}

// WriteEvent writes the marshaled JSON object on a new line, writing a comma
// first if needed to delimit the previous object.
func (s *eventWriter) WriteEvent(e *Event) error {
	delim := ",\n"
	if !s.wroteFirst {
		delim = "\n"
		s.wroteFirst = true
	}
	if _, err := io.WriteString(s.w, delim); err != nil {
		return fmt.Errorf("write event delimiter: %w", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if _, err := s.w.Write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}
