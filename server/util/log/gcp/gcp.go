package gcp

import (
	"context"
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"cloud.google.com/go/logging"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/structpb"

	logpb "google.golang.org/genproto/googleapis/logging/v2"
)

var (
	ProjectID = flag.String("app.log_gcp_project_id", "", "The project ID to log to in GCP (if any).")
	LogID     = flag.String("app.log_gcp_log_id", "", "The log ID to log to in GCP (if any).")
)

const (
	SourceLocationFieldName = "logging.googleapis.com/sourceLocation"
)

func NewLogWriter() (zerolog.LevelWriter, error) {
	if *ProjectID == "" || *LogID == "" {
		return nil, nil
	}
	client, err := logging.NewClient(context.Background(), *ProjectID)
	if err != nil {
		return nil, err
	}
	return &logWriter{ctx: context.Background(), logger: client.Logger(*LogID)}, nil
}

type logWriter struct {
	ctx    context.Context
	logger *logging.Logger
}

func (l *logWriter) Write(p []byte) (int, error) {
	l.logger.StandardLogger(logging.Default).Print(string(p))
	return len(p), nil
}

func zerologLevelToGCPSeverity(l zerolog.Level) logging.Severity {
	switch l {
	case zerolog.DebugLevel:
		return logging.Debug
	case zerolog.InfoLevel:
		return logging.Info
	case zerolog.WarnLevel:
		return logging.Warning
	case zerolog.ErrorLevel:
		return logging.Error
	case zerolog.PanicLevel:
		fallthrough
	case zerolog.FatalLevel:
		return logging.Critical
	default:
		return logging.Default
	}
}

func jsonPayload(p []byte) (*structpb.Struct, error) {
	m := map[string]any{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}
	jsonPayload, err := structpb.NewStruct(m)
	if err != nil {
		return nil, err
	}
	return jsonPayload, nil
}

func populateEntryFromJsonPayload(entry *logging.Entry, payload *structpb.Struct) {
	entry.Payload = payload

	// Populate the Entry fields, as this won't happen automatically. The map of
	// json to Entry fields can be found here:
	// https://cloud.google.com/logging/docs/structured-logging
	fields := payload.GetFields()
	if t, err := time.Parse(zerolog.TimeFieldFormat, fields[zerolog.TimestampFieldName].GetStringValue()); err == nil {
		entry.Timestamp = t
	}
	if v, ok := fields[SourceLocationFieldName]; ok {
		line, _ := strconv.ParseInt(v.GetStructValue().GetFields()["line"].GetStringValue(), 10, 64)
		entry.SourceLocation = &logpb.LogEntrySourceLocation{
			File:     v.GetStructValue().GetFields()["file"].GetStringValue(),
			Line:     line,
			Function: v.GetStructValue().GetFields()["function"].GetStringValue(),
		}
	}
}

func (l *logWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	entry := logging.Entry{}
	entry.Severity = zerologLevelToGCPSeverity(level)
	if payload, err := jsonPayload(p); err == nil {
		populateEntryFromJsonPayload(&entry, payload)
		// remove fields from the payload that are represented in the Entry
		delete(payload.GetFields(), zerolog.TimestampFieldName)
		delete(payload.GetFields(), SourceLocationFieldName)
		delete(payload.GetFields(), zerolog.LevelFieldName)
	} else {
		entry.Payload = string(p)
	}
	if level == zerolog.PanicLevel || level == zerolog.FatalLevel {
		l.logger.LogSync(l.ctx, entry)
	} else {
		l.logger.Log(entry)
	}
	return len(p), nil
}
