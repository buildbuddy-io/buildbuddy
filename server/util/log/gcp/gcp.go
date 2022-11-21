package gcp

import (
	"context"
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"cloud.google.com/go/logging"
	"github.com/rs/zerolog"

	logpb "google.golang.org/genproto/googleapis/logging/v2"
)

var (
	ProjectID = flag.String("app.log_gcp_project_id", "", "The project ID to log to in GCP (if any).")
	LogID     = flag.String("app.log_gcp_log_id", "", "The log ID to log to in GCP (if any).")
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

func (l *logWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	entry := logging.Entry{}
	m := map[string]any{}
	if err := json.Unmarshal(p, &m); err == nil {
		if v, ok := m[zerolog.TimestampFieldName]; ok {
			if t, ok := v.(string); ok {
				if entry.Timestamp, err = time.Parse(zerolog.TimeFieldFormat, t); err != nil {
					entry.Timestamp = time.Time{}
				}
			}
		}
		if v, ok := m[zerolog.MessageFieldName]; ok {
			if p, ok := v.(string); ok {
				entry.Payload = p
			}
		}
		if v, ok := m["logging.googleapis.com/sourceLocation"]; ok {
			if m, ok := v.(map[string]any); ok {
				entry.SourceLocation = &logpb.LogEntrySourceLocation{}
				if v, ok := m["file"]; ok {
					if f, ok := v.(string); ok {
						entry.SourceLocation.File = f
					}
				}
				if v, ok := m["line"]; ok {
					if l, ok := v.(string); ok {
						if n, err := strconv.ParseInt(l, 10, 64); err != nil {
							entry.SourceLocation.Line = n
						}
					}
				}
			}
		}
	} else {
		entry.Payload = string(p)
	}
	switch level {
	case zerolog.DebugLevel:
		entry.Severity = logging.Debug
		l.logger.Log(entry)
	case zerolog.InfoLevel:
		entry.Severity = logging.Info
		l.logger.Log(entry)
	case zerolog.WarnLevel:
		entry.Severity = logging.Warning
		l.logger.Log(entry)
	case zerolog.ErrorLevel:
		entry.Severity = logging.Error
		l.logger.Log(entry)
	case zerolog.PanicLevel:
		fallthrough
	case zerolog.FatalLevel:
		entry.Severity = logging.Critical
		l.logger.LogSync(l.ctx, entry)
	default:
		entry.Severity = logging.Default
		l.logger.Log(entry)
	}
	return len(p), nil
}
