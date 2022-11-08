package gcp

import (
	"context"

	"cloud.google.com/go/logging"
	"github.com/rs/zerolog"
)

func NewLogger(projectID string, logID string) (zerolog.LevelWriter, error) {
	client, err := logging.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}
	return &logWriter{ctx: context.Background(), logger: client.Logger(logID)}, nil
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
	switch level {
	case zerolog.DebugLevel:
		l.logger.StandardLogger(logging.Debug).Print(string(p))
	case zerolog.InfoLevel:
		l.logger.StandardLogger(logging.Info).Print(string(p))
	case zerolog.WarnLevel:
		l.logger.StandardLogger(logging.Warning).Print(string(p))
	case zerolog.ErrorLevel:
		l.logger.StandardLogger(logging.Error).Print(string(p))
	case zerolog.PanicLevel:
		fallthrough
	case zerolog.FatalLevel:
		l.logger.StandardLogger(logging.Critical).Print(string(p))
		l.logger.Flush()
	default:
		l.logger.StandardLogger(logging.Default).Print(string(p))
	}
	return len(p), nil
}
