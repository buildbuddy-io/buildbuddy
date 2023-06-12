package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	remoteExecutor = flag.String("remote_executor", "grpc://localhost:1985", "Execution service target.")
	instanceName   = flag.String("remote_instance_name", "", "Remote instance name.")
	inputRoot      = flag.String("input_root", "", "Input root directory. Defaults to an empty dir.")
)

var exitCode = 0

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
	os.Exit(exitCode)
}

func run() error {
	ctx := context.Background()
	args := flag.Args()
	if *inputRoot == "" {
		dir, err := os.MkdirTemp("", "")
		if err != nil {
			return fmt.Errorf("make temp dir: %w", err)
		}
		defer os.RemoveAll(dir)
		*inputRoot = dir
	}
	conn, err := grpc_client.DialTarget(*remoteExecutor)
	if err != nil {
		return err
	}
	defer conn.Close()
	env := real_environment.NewRealEnv(nil /*=healthChecker*/)
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	ad, err := createAction(ctx, env, *inputRoot, args)
	if err != nil {
		return err
	}
	exec := env.GetRemoteExecutionClient()
	req := &repb.ExecuteRequest{
		ActionDigest:    ad,
		SkipCacheLookup: true,
	}
	stream, err := exec.Execute(ctx, req)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	taskID := ""
	for {
		msg, err := stream.Recv()
		if err == io.EOF || status.IsUnavailableError(err) {
			// EOF is unexpected; it likely means we disconnected from the
			// server. Attempt to reconnect using WaitExecution.
			if taskID == "" {
				log.Errorf("Execute failed: %s", err)
				break
			}
			log.Infof("Lost connection to app; re-connecting using WaitExecution.")
			req := &repb.WaitExecutionRequest{Name: taskID}
			r := retry.DefaultWithContext(ctx)
			var reconnectErr error
			for r.Next() {
				stream, err = exec.WaitExecution(ctx, req)
				if err != nil {
					reconnectErr = err
					continue
				}
				reconnectErr = nil
				break
			}
			if reconnectErr != nil {
				return reconnectErr
			}
			continue
		}
		if err != nil {
			return err
		}
		if msg.GetError() != nil {
			if err := gstatus.ErrorProto(msg.GetError()); err != nil {
				return err
			}
		}
		if taskID == "" {
			taskID = msg.GetName()
			log.Infof("Started task %s", taskID)
		}
		if !msg.GetDone() {
			continue
		}
		res := &repb.ExecuteResponse{}
		if err := msg.GetResponse().UnmarshalTo(res); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
		ar := res.GetResult()
		if err := cachetools.GetBlob(ctx, env.GetByteStreamClient(), digest.NewResourceName(ar.GetStdoutDigest(), *instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256), os.Stdout); err != nil {
			log.Infof("Failed to fetch stdout: %s", err)
		}
		if err := cachetools.GetBlob(ctx, env.GetByteStreamClient(), digest.NewResourceName(ar.GetStderrDigest(), *instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256), os.Stderr); err != nil {
			log.Infof("Failed to fetch stderr: %s", err)
		}
		if ar.GetExitCode() >= 0 {
			exitCode = int(ar.GetExitCode())
		}
		break
	}
	return nil
}

func createAction(ctx context.Context, env environment.Env, inputRoot string, args []string) (*repb.Digest, error) {
	command := &repb.Command{Arguments: args}
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), *instanceName, repb.DigestFunction_SHA256, command)
	if err != nil {
		return nil, err
	}
	ird, _, err := cachetools.UploadDirectoryToCAS(ctx, env, *instanceName, repb.DigestFunction_SHA256, inputRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to upload input root: %w", err)
	}
	action := &repb.Action{
		InputRootDigest: ird,
		CommandDigest:   cd,
	}
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), *instanceName, repb.DigestFunction_SHA256, action)
	if err != nil {
		return nil, err
	}
	return ad, nil
}
