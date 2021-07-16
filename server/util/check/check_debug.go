package check

import "github.com/buildbuddy-io/buildbuddy/server/util/log"

func DebugFail(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
}
