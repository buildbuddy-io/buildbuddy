---
id: enterprise-api
title: API Documentation
sidebar_label: Enterprise API
---

The BuildBuddy API let's you programmatically obtain information about your Bazel builds. API access available to Enterprise BuildBuddy Customers. 

Requests can be made via JSON or using Protobuf. The examples below are using the JSON API. For a full overview of the service, you can view the [service definition](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/service.proto) or the [individual protos](https://github.com/buildbuddy-io/buildbuddy/tree/master/proto/api/v1).

## GetInvocation
The `GetInvocation` endpoint allows you to fetch invocations associated with a commit SHA or invocation ID. View full [Invocation proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/invocation.proto).

### Endpoint
```
https://app.buildbuddy.io/api/v1/GetInvocation
```

### Service
```protobuf
// Retrieves a list of invocations or a specific invocation matching the given
// request selector.
rpc GetInvocation(GetInvocationRequest) returns (GetInvocationResponse);
```

### Example cURL request

```bash
curl -d '{"selector": {"invocation_id":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845"}}' \
  -H 'x-buildbuddy-api-key: YOUR_BUILDBUDDY_API_KEY' \
  -H 'Content-Type: application/json' \
  https://app.buildbuddy.io/api/v1/GetInvocation
```

Make sure to replace `YOUR_BUILDBUDDY_API_KEY` and the invocation ID `c6b2b6de-c7bb-4dd9-b7fd-a530362f0845` with your own values.


### Example cURL response
```json
{
   "invocation":[
      {
         "id":{
            "invocationId":"c7fbfe97-8298-451f-b91d-722ad91632ea"
         },
         "success":true,
         "user":"runner",
         "durationUsec":"221970000",
         "host":"fv-az278-49",
         "command":"build",
         "pattern":"//...",
         "actionCount":"1402",
         "createdAtUsec":"1623193638545989",
         "updatedAtUsec":"1623193638545989",
         "repoUrl":"https://github.com/buildbuddy-io/buildbuddy",
         "commitSha":"800f549937a4c0a1614e65501caf7577d2a00624",
         "role":"CI"
      }
   ]
}
```

### GetInvocationRequest

```protobuf
// Request passed into GetInvocation
message GetInvocationRequest {
  // The selector defining which invocations(s) to retrieve.
  InvocationSelector selector = 1;

  // The next_page_token value returned from a previous request, if any.
  string page_token = 3;
}
```

### GetInvocationResponse

```protobuf
// Response from calling GetInvocation
message GetInvocationResponse {
  // Invocations matching the request invocation, possibly capped by a
  // server limit.
  repeated Invocation invocation = 1;

  // Token to retrieve the next page of results, or empty if there are no
  // more results in the list.
  string next_page_token = 2;
}
```

### InvocationSelector

```protobuf
// The selector used to specify which invocations to return.
message InvocationSelector {
  // One invocation_id or commit_sha is required.

  // Optional: The Invocation ID.
  // Return only the invocation with this invocation ID.
  string invocation_id = 1;

  // Optional: The commmit SHA.
  // If set, only the invocations with this commit SHA will be returned.
  string commit_sha = 2;
}
```

### Invocation
```protobuf
// Response from calling GetInvocation
message GetInvocationResponse {
  // Invocations matching the request invocation, possibly capped by a
  // server limit.
  repeated Invocation invocation = 1;

  // Token to retrieve the next page of results, or empty if there are no
  // more results in the list.
  string next_page_token = 2;
}

// Each Invocation represents metadata associated with a given invocation.
message Invocation {
  // The resource ID components that identify the Invocation.
  message Id {
    // The Invocation ID.
    string invocation_id = 1;
  }

  // The resource ID components that identify the Invocation.
  Id id = 1;

  // Whether or not the build was successful.
  bool success = 3;

  // The user who performed this build.
  string user = 4;

  // The duration of this build, from start to finish.
  int64 duration_usec = 5;

  // The host this build was executed on.
  string host = 6;

  // The command performed (usually "build" or "test").
  string command = 7;

  // The build patterns specified for this build.
  string pattern = 8;

  // The number of actions performed.
  int64 action_count = 9;

  // The time this invocation was created and updated, respectively. Invocations
  // are created as soon as the first event is received from the client and
  // updated with subsequent events until they are finalized.
  int64 created_at_usec = 13;
  int64 updated_at_usec = 14;

  // A URL to the git repo this invocation was for.
  string repo_url = 15;

  // The commit SHA that this invocation was for.
  string commit_sha = 16;

  // The role played by this invocation. Ex: "CI"
  string role = 19;
}
```






## GetTarget
The `GetTarget` endpoint allows you to fetch targets associated with a given invocation ID. View full [Target proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/target.proto).

### Endpoint
```
https://app.buildbuddy.io/api/v1/GetTarget
```

### Service
```protobuf
// Retrieves a list of targets or a specific target matching the given
// request selector.
rpc GetTarget(GetTargetRequest) returns (GetTargetResponse);
```

### Example cURL request

```bash
curl -d '{"selector": {"invocation_id":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845"}}' \
  -H 'x-buildbuddy-api-key: YOUR_BUILDBUDDY_API_KEY' \
  -H 'Content-Type: application/json' \
  https://app.buildbuddy.io/api/v1/GetTarget
```

Make sure to replace `YOUR_BUILDBUDDY_API_KEY` and the invocation ID `c6b2b6de-c7bb-4dd9-b7fd-a530362f0845` with your own values.


### Example cURL response
```json
{
   "target":[
      {
         "id":{
            "invocationId":"c7fbfe97-8298-451f-b91d-722ad91632ea",
            "targetId":"aWQ6OnYxOjovL3Rvb2xzL3JlcGxheV9hY3Rpb246cmVwbGF5X2FjdGlvbl9saWI"
         },
         "label":"//tools/replay_action:replay_action_lib",
         "status":"BUILT",
         "ruleType":"go_library"
      },
      ...
      {
         "id":{
            "invocationId":"c7fbfe97-8298-451f-b91d-722ad91632ea",
            "targetId":"aWQ6OnYxOjovL2VudGVycHJpc2UvYXBwOmNvcHlfYXBwX2J1bmRsZV9zb3VyY2VtYXA"
         },
         "label":"//enterprise/app:copy_app_bundle_sourcemap",
         "status":"BUILT",
         "ruleType":"genrule"
      },
      {
         "id":{
            "invocationId":"c7fbfe97-8298-451f-b91d-722ad91632ea",
            "targetId":"aWQ6OnYxOjovL2VudGVycHJpc2U6YnVpbGRidWRkeQ"
         },
         "label":"//enterprise:buildbuddy",
         "status":"BUILT",
         "ruleType":"go_binary"
      }
   ]
}
```

### GetTargetRequest

```protobuf
// Request passed into GetTarget
message GetTargetRequest {
  // The selector defining which target(s) to retrieve.
  TargetSelector selector = 1;

  // The next_page_token value returned from a previous request, if any.
  string page_token = 3;
}
```

### GetTargetResponse

```protobuf
// Response from calling GetTarget
message GetTargetResponse {
  // Targets matching the request invocation, possibly capped by a
  // server limit.
  repeated Target target = 1;

  // Token to retrieve the next page of results, or empty if there are no
  // more results in the list.
  string next_page_token = 2;
}
```

### TargetSelector

```protobuf
// The selector used to specify which targets to return.
message TargetSelector {
  // Required: The Invocation ID.
  // All actions returned will be scoped to this invocation.
  string invocation_id = 1;

  // Optional: The Target ID.
  // If set, only the target with this target id will be returned.
  string target_id = 2;
}
```

### Target
```protobuf
// Each Target represents data for a given target in a given Invocation.
message Target {
  // The resource ID components that identify the Target.
  message Id {
    // The Invocation ID.
    string invocation_id = 1;

    // The Target ID.
    string target_id = 2;
  }

  // The resource ID components that identify the Target.
  Id id = 1;

  // The label of the target Ex: //server/test:foo
  string label = 2;

  // The aggregate status of the target.
  Status status = 3;

  // When this target started and its duration.
  Timing timing = 4;

  // The type of the target rule. Ex: java_binary
  string rule_type = 5;

  // Tags applied to this target (if any).
  repeated string tag = 6;
}
```






## GetAction
The `GetAction` endpoint allows you to fetch actions associated with a given target or invocation. View full [Action proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/action.proto).

### Endpoint
```
https://app.buildbuddy.io/api/v1/GetAction
```

### Service
```protobuf
// Retrieves a list of targets or a specific target matching the given
// request selector.
rpc GetAction(GetActionRequest) returns (GetActionResponse);
```

### Example cURL request

```bash
curl -d '{"selector": {"invocation_id":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845"}}' \
  -H 'x-buildbuddy-api-key: YOUR_BUILDBUDDY_API_KEY' \
  -H 'Content-Type: application/json' \
  https://app.buildbuddy.io/api/v1/GetAction
```

Make sure to replace `YOUR_BUILDBUDDY_API_KEY` and the invocation ID `c6b2b6de-c7bb-4dd9-b7fd-a530362f0845` with your own values.


### Example cURL response
```json
{
   "action":[
      {
         "id":{
            "invocationId":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845",
            "targetId":"aWQ6OnYxOjovLzpzdGF0aWM",
            "configurationId":"00e90e1ab7325d5e63d03bfe5f808477b2bb66ca6ae9af26c036cae67ee81cf9",
            "actionId":"aWQ6OnYxOjpidWlsZA"
         }
      },
      [...]
      {
         "id":{
            "invocationId":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845",
            "targetId":"aWQ6OnYxOjovL2VudGVycHJpc2UvYXBwOnN0eWxl",
            "configurationId":"00e90e1ab7325d5e63d03bfe5f808477b2bb66ca6ae9af26c036cae67ee81cf9",
            "actionId":"aWQ6OnYxOjpidWlsZA"
         },
         "file":[
            {
               "name":"enterprise/app/style.css",
               "uri":"bytestream://cloud.buildbuddy.io/buildbuddy-io/buildbuddy-internal/ci/blobs/e21b1e3411792e17e698be879a3548527d620c65953986c96d5a81f933e776aa/68837"
            }
         ]
      },
      {
         "id":{
            "invocationId":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845",
            "targetId":"aWQ6OnYxOjovLzp2ZXQ",
            "configurationId":"9a01374ae0e8164eec90f708e7a997520994e71b433a5265c89582c4490d75e9",
            "actionId":"aWQ6OnYxOjpidWlsZA"
         },
         "file":[
            {
               "name":"vet_/vet",
               "uri":"bytestream://cloud.buildbuddy.io/buildbuddy-io/buildbuddy-internal/ci/blobs/915edf6aca4bd4eac3e4602641b0633a7aaf038d62d5ae087884a2d8acf0926a/7029420"
            }
         ]
      },
      {
         "id":{
            "invocationId":"c6b2b6de-c7bb-4dd9-b7fd-a530362f0845",
            "targetId":"aWQ6OnYxOjovL2VudGVycHJpc2Uvc2VydmVyL3Rlc3QvaW50ZWdyYXRpb24vcmVtb3RlX2NhY2hlOnJlbW90ZV9jYWNoZV90ZXN0",
            "configurationId":"00e90e1ab7325d5e63d03bfe5f808477b2bb66ca6ae9af26c036cae67ee81cf9",
            "actionId":"aWQ6OnYxOjp0ZXN0LVNfMy1SXzEtQV8x"
         },
         "file":[
            {
               "name":"test.log",
               "uri":"bytestream://cloud.buildbuddy.io/buildbuddy-io/buildbuddy-internal/ci/blobs/09e6fe6e1fd8c8734339a0a84c3c7a0eb121b57a45d21cfeb1f265bffe4c4888/216"
            }
         ]
      }
   ]
}
```

### GetActionRequest

```protobuf
// Request passed into GetAction
message GetActionRequest {
  // The selector defining which action(s) to retrieve.
  ActionSelector selector = 1;

  // The next_page_token value returned from a previous request, if any.
  string page_token = 2;
}
```

### GetActionResponse

```protobuf
// Response from calling GetAction
message GetActionResponse {
  // Actions matching the request, possibly capped a server limit.
  repeated Action action = 1;

  // Token to retrieve the next page of results, or empty if there are no
  // more results in the list.
  string next_page_token = 2;
}
```

### ActionSelector

```protobuf
// The selector used to specify which actions to return.
message ActionSelector {
  // Required: The Invocation ID.
  // All actions returned will be scoped to this invocation.
  string invocation_id = 1;

  // Optional: The Target ID.
  // If set, all actions returned will be scoped to this target.
  string target_id = 2;

  // Optional: The Configuration ID.
  // If set, all actions returned will be scoped to this configuration.
  string configuration_id = 3;

  // Optional: The Action ID.
  // If set, only the action with this action id will be returned.
  string action_id = 4;
}
```

### Action
```protobuf
// An action that happened as part of a configured target. This action could be
// a build, a test, or another type of action.
message Action {
  // The resource ID components that identify the Action.
  message Id {
    // The Invocation ID.
    string invocation_id = 1;

    // The Target ID.
    string target_id = 2;

    // The Configuration ID.
    string configuration_id = 3;

    // The Action ID.
    string action_id = 4;
  }

  // The resource ID components that identify the Action.
  Id id = 1;

  // A list of file references for action level files.
  repeated File file = 2;
}
```






## GetFile
The `GetFile` endpoint allows you to fetch files associated with a given url. View full [File proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/file.proto).

### Endpoint
```
https://app.buildbuddy.io/api/v1/GetFile
```

### Service
```protobuf
// Streams the File with the given uri.
// - Over gRPC returns a stream of bytes to be stitched together in order.
// - Over HTTP this simply returns the requested file.
rpc GetFile(GetFileRequest) returns (stream GetFileResponse);
```

### Example cURL request

```bash
curl -d '{"uri":"bytestream://cloud.buildbuddy.io/buildbuddy-io/buildbuddy-internal/ci/blobs/09e6fe6e1fd8c8734339a0a84c3c7a0eb121b57a45d21cfeb1f265bffe4c4888/216"}' \
  -H 'x-buildbuddy-api-key: YOUR_BUILDBUDDY_API_KEY' \
  -H 'Content-Type: application/json' \
  https://app.buildbuddy.io/api/v1/GetFile
```

Make sure to replace `YOUR_BUILDBUDDY_API_KEY` and the file uri `bytestream://cloud.buildbuddy.io/buildbuddy-io/buildbuddy-internal/ci/blobs/09e6fe6e1fd8c8734339a0a84c3c7a0eb121b57a45d21cfeb1f265bffe4c4888/216` with your own values.


### Example cURL response
The file contents.
```
exec ${PAGER:-/usr/bin/less} "$0" || exit 1
Executing tests from //enterprise/server/test/integration/remote_cache:remote_cache_test
-----------------------------------------------------------------------------
PASS
```

### GetFileRequest

```protobuf
// Request object for GetFile
message GetFileRequest {
  // This corresponds to the uri field in the File message.
  string uri = 1;
}
```

### GetFileResponse

```protobuf
// Response object for GetFile
message GetFileResponse {
  // The file data.
  bytes data = 1;
}
```

### File

```protobuf
// A file associated with a BuildBuddy build.
message File {
  string name = 1;
  string uri = 2;
}
```
