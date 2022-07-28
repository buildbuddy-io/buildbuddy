// Package dsingleflight is a distributed version of the singleflight package.
// Work is coordinated across servers using Redis.
//
// Usage:
//     sf := dsingleflight.New(rdb)
//     result, err := df.Do(ctx, "executor-large-compute-CBF43926", func() ([]byte, error) {
// 		   return expensiveCalculation()
//     })
//
// Considerations:
//  - Deduplication is best-effort and depends on redis availability. There's a
//    possibility that a task may be executed more than once.
//  - Task keys are not namespaced. Choose keys that will be unique across all
//    servers connected to the Redis instance/cluster and that appropriately
//    differentiate different types of tasks.
//    An example key might be servertype-subsystem-taskhash
//  - The result is temporarily stored in Redis, so it must be serializable to
//    a slice of bytes.

package dsingleflight

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const (
	resultStatusField = "status"
	resultDataField   = "data"

	lockTTL   = 5 * time.Second
	resultTTL = 30 * time.Second
)

type Coordinator struct {
	rdb redis.UniversalClient
}

func New(rdb redis.UniversalClient) *Coordinator {
	return &Coordinator{rdb: rdb}
}

type result struct {
	data []byte
	err  error
}

type Work func() ([]byte, error)

func redisLockKey(workKey string) string {
	return fmt.Sprintf("singleflightLock/{%s}", workKey)
}

func redisResultKey(workKey string) string {
	return fmt.Sprintf("singleflightResult/{%s}", workKey)
}

// doWork executes the work function and publishes the result to redis as well
// as returning it to teh caller.
func (c *Coordinator) doWork(ctx context.Context, workKey string, work Work) ([]byte, error) {
	r, workErr := work()
	errStatus := gstatus.Convert(workErr)
	errStatusBytes, err := proto.Marshal(errStatus.Proto())
	if err != nil {
		return nil, status.UnknownErrorf("could not marshal status: %s", err)
	}

	p := c.rdb.TxPipeline()

	resultKey := redisResultKey(workKey)
	if err := p.XAdd(ctx, &redis.XAddArgs{
		Stream: resultKey,
		Values: []string{resultStatusField, string(errStatusBytes), resultDataField, string(r)},
	}).Err(); err != nil {
		return nil, status.UnavailableErrorf("could not publish result: %s", err)
	}
	if err := p.Expire(ctx, resultKey, resultTTL).Err(); err != nil {
		return nil, status.UnavailableErrorf("could not update result ttl: %s", err)
	}

	if _, err := p.Exec(ctx); err != nil {
		return nil, status.UnavailableErrorf("could not store result: %s", err)
	}

	return r, workErr
}

// claimWork loops trying to lock the workKey. If locking succeeds, the passed
// work function is executed and its result returned. While the
// work function is executing, the TTL on the lock is extended to prevent
// anyone else from claiming the work.
func (c *Coordinator) claimWork(ctx context.Context, workKey string, work Work) ([]byte, error) {
	workResult := make(chan *result, 1)

	firstCheck := true
	isOwner := false
	lockKey := redisLockKey(workKey)
	for {
		if !isOwner {
			claimed, err := c.rdb.SetNX(ctx, lockKey, "meow", lockTTL).Result()
			if err != nil {
				return nil, status.UnavailableErrorf("could not lock work: %s, ", err)
			}
			if claimed {
				log.CtxInfof(ctx, "dsingleflight: we claimed %q, starting work", workKey)
				isOwner = true
				go func() {
					data, err := c.doWork(ctx, workKey, work)
					workResult <- &result{data: data, err: err}
				}()
			} else if firstCheck {
				firstCheck = false
				log.CtxInfof(ctx, "dsingleflight: waiting for result for %q", workKey)
			}
		} else {
			if err := c.rdb.Expire(ctx, lockKey, lockTTL).Err(); err != nil {
				return nil, status.UnavailableErrorf("could not update lock expiry: %s", err)
			}
		}

		select {
		case result := <-workResult:
			log.CtxInfof(ctx, "dsingleflight: work completed for %q", workKey)
			return result.data, result.err
		case <-time.After(1 * time.Second):
		}
	}
}

// waitResult blocks until result is retrieved from redis or the context is
// cancelled.
func (c *Coordinator) waitResult(ctx context.Context, workKey string) ([]byte, error) {
	resultKey := redisResultKey(workKey)
	data, err := c.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{resultKey, "0"},
		Count:   1,
		Block:   0, // Block indefinitely
	}).Result()
	if err != nil {
		return nil, status.UnavailableErrorf("could not read from stream: %s", err)
	}

	if len(data) != 1 {
		return nil, status.FailedPreconditionErrorf("expected exactly one stream")
	}
	if len(data[0].Messages) != 1 {
		return nil, status.FailedPreconditionErrorf("expected exactly one message")
	}

	log.CtxInfof(ctx, "dsingleflight: got result for %q", workKey)

	vals := data[0].Messages[0].Values
	if s, ok := vals[resultStatusField].(string); ok {
		var statusProto spb.Status
		if err := proto.Unmarshal([]byte(s), &statusProto); err != nil {
			return nil, status.UnknownErrorf("could not unmarshal status: %s", err)
		}
		if err := gstatus.ErrorProto(&statusProto); err != nil {
			return nil, err
		}
	}

	if data, ok := vals[resultDataField].(string); ok {
		return []byte(data), nil
	} else {
		return nil, status.UnknownErrorf("result did not contain error or data")
	}
}

// Do executes the given function, making a best effort attempt to only execute
// the work once amongst concurrent callers for the same key.
func (c *Coordinator) Do(ctx context.Context, key string, work Work) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workResultCh := make(chan *result, 1)
	waitResultCh := make(chan *result, 1)

	go func() {
		data, err := c.claimWork(ctx, key, work)
		workResultCh <- &result{data: data, err: err}
	}()
	go func() {
		data, err := c.waitResult(ctx, key)
		waitResultCh <- &result{data: data, err: err}
	}()

	select {
	case result := <-workResultCh:
		return result.data, result.err
	case result := <-waitResultCh:
		return result.data, result.err
	}
}

func (c *Coordinator) DoProto(ctx context.Context, key string, work func() (proto.Message, error), out proto.Message) error {
	bs, err := c.Do(ctx, key, func() ([]byte, error) {
		m, err := work()
		if err != nil {
			return nil, err
		}
		return proto.Marshal(m)
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(bs, out)
}
