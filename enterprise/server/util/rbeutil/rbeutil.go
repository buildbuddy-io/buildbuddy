package rbeutil

import "strings"

const (
	// Certain RBE features rely on predictable names for resources so that different apps can co-operate
	// without needing any direct communication or shared state. For example, a client may be waiting for execution
	// progress updates on one app and updates may be published by other apps. This co-operation is possible because
	// all the apps generate the same PubSub channel name based on the execution ID.
	// Occasionally we need to introduce changes that affect these shared resources. To be able to roll out these
	// changes safely, we encode the scheme being used as part of the execution ID. This allows apps to determine which
	// naming/protocol they should for a given execution without knowing anything about other apps.

	// V2ExecutionIDPrefix is used for rolling out changes to the Redis PubSub stream keys used for remote execution
	// status updates and for task key updates. Notably under the new scheme both the PubSub stream keys and the task
	// keys use the task ID as the hash for the ring client to ensure that PubSub channel and task metadata for a single
	// task are placed on the same Redis shard. The other change is that we publish a dummy marker message to the PubSub
	// stream when the PubSub channel is created so that we can properly detect whether the PubSub stream has
	// disappeared from redis.
	V2ExecutionIDPrefix = "v2/"
)

func IsV2ExecutionID(executionID string) bool {
	return strings.HasPrefix(executionID, V2ExecutionIDPrefix)
}
