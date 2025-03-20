package main

import (
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"google.golang.org/protobuf/encoding/protojson"

	statspb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	m := &statspb.GetTrendResponse{}
	if err := proto.Unmarshal(b, m); err != nil {
		return err
	}
	jb, err := protojson.MarshalOptions{Multiline: true}.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Println(string(jb))
	return nil
}
