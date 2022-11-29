package gcp

import (
	"context"
	"encoding/json"
	"flag"

	"cloud.google.com/go/logging"
	"github.com/rs/zerolog"

	"google.golang.org/protobuf/types/known/structpb"
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

func (l *logWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	entry := logging.Entry{}
	if payload, err := jsonPayload(p); err == nil {
		entry.Payload = payload
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
