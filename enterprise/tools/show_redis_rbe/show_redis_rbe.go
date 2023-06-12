package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()
	r := redis.NewClient(redisutil.TargetToOptions("redis://localhost:6379"))

	taskKeys, err := r.Keys(ctx, "task/*").Result()
	if err != nil {
		return err
	}
	for _, k := range taskKeys {
		fmt.Printf("%s:\n", k)
		h, err := r.HGetAll(ctx, k).Result()
		if err != nil {
			fmt.Printf("  error: %q\n", err)
			continue
		}
		hkeys := maps.Keys(h)
		sort.Strings(hkeys)
		for _, k := range hkeys {
			v := h[k]
			if k == "taskProto" {
				task := &repb.ExecutionTask{}
				if err := proto.Unmarshal([]byte(v), task); err != nil {
					v = fmt.Sprintf("<error: unmarshal: %s>", err)
				} else {
					v = fmt.Sprintf("<binary(protobuf):{Command:{Arguments: [%s, ...], ...}, ...}>", task.GetCommand().GetArguments()[0])
				}
			} else if k == "schedulingMetadataProto" {
				// TODO: show a summary
				v = "<binary(protobuf):{...}>"
			}
			fmt.Printf("  %s: %s\n", k, v)
		}
	}
	return nil
}
