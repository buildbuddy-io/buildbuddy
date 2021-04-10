package eventlog

import (
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	enabled = flag.Bool("enable_event_log", false, "")
	logFile io.Writer
)

func init() {
	path := "/tmp/bb_events.log"
	var err error
	logFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}
}

func Log(name string, kvs ...string) {
	if !*enabled {
		return
	}
	if len(kvs)%2 != 0 {
		panic("Bad eventlog.Log() call")
	}
	out := "{"
	out += fmt.Sprintf("%q: %q", "event", name)
	for i := 0; i < len(kvs); i += 2 {
		k := kvs[i]
		v := kvs[i+1]
		out += fmt.Sprintf(", %q: %q", k, v)
	}
	out += "}\n"
	logFile.Write([]byte(out))
}
