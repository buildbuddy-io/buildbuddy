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
	V2ExecutionIDPrefix = "v2/"
)

func IsV2ExecutionID(executionID string) bool {
	return strings.HasPrefix(executionID, V2ExecutionIDPrefix)
}
