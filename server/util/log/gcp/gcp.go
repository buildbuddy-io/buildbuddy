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
		l.logger.Log(logging.Entry{
			Severity: logging.Debug,
			Payload:  p,
		})
	case zerolog.InfoLevel:
		l.logger.Log(logging.Entry{
			Severity: logging.Info,
			Payload:  p,
		})
	case zerolog.WarnLevel:
		l.logger.Log(logging.Entry{
			Severity: logging.Warning,
			Payload:  p,
		})
	case zerolog.ErrorLevel:
		l.logger.Log(logging.Entry{
			Severity: logging.Error,
			Payload:  p,
		})
	case zerolog.PanicLevel:
		fallthrough
	case zerolog.FatalLevel:
		l.logger.LogSync(l.ctx, logging.Entry{
			Severity: logging.Critical,
			Payload:  p,
		})
	default:
		l.logger.Log(logging.Entry{
			Severity: logging.Default,
			Payload:  p,
		})
	}
	return len(p), nil
}
