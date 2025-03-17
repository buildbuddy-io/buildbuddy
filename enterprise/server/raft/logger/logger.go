package logger

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/rs/zerolog"
)

// Import this class for the side effect of quieting the
// raft logger.

type dbCompatibleLogger struct {
	log.Logger
}

// Don't panic in server code.
func (l *dbCompatibleLogger) Panicf(format string, args ...interface{}) {
	l.Errorf(format, args...)
}

// Ignore SetLevel commands.
func (l *dbCompatibleLogger) SetLevel(level logger.LogLevel) {}

func init() {
	logger.SetLoggerFactory(func(pkgName string) logger.ILogger {
		l := log.NamedSubLogger(pkgName)
		// Make the raft library be quieter.
		switch pkgName {
		case "raft", "dragonboat", "logdb", "raftpb", "transport":
			l = l.Level(zerolog.Disabled)
		case "rsm":
			l = l.Level(zerolog.InfoLevel)
		}
		return &dbCompatibleLogger{l}
	})
}
