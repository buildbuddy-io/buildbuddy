package main

import (
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"google.golang.org/protobuf/encoding/protojson"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	messageType = flag.String("type", "", "Message type: ExecuteResponse|...")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	var m proto.Message
	switch *messageType {
	case "ExecuteResponse":
		m = &repb.ExecuteResponse{}
	default:
		return fmt.Errorf("unknown type %q", *messageType)
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return err
	}
	jb, err := protojson.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}
	if _, err := os.Stdout.Write(jb); err != nil {
		return fmt.Errorf("write to stdout: %w", err)
	}
	return nil
}
