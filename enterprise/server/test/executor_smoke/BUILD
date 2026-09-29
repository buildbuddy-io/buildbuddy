load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Smoke tests an executor binary against a fake, in-process app. See
# executor_smoke_test.go for how to run it against a prebuilt binary.
go_test(
    name = "executor_smoke_test",
    size = "medium",
    srcs = [
        "executor_smoke_test.go",
        "fake_app_test.go",
        "helper_test.go",
    ],
    data = ["//enterprise/server/cmd/executor"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
    },
    x_defs = {
        "executorRlocationpath": "$(rlocationpath //enterprise/server/cmd/executor)",
    },
    deps = [
        "//enterprise/server/remote_execution/operation",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:worker_go_proto",
        "//server/backends/memory_cache",
        "//server/nullauth",
        "//server/real_environment",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/capabilities_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/util/healthcheck",
        "//server/util/prefix",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)
