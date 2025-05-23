load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hostedrunner",
    srcs = ["hostedrunner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner",
    deps = [
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/ci_runner_util",
        "//enterprise/server/workflow/config",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/endpoint_urls/cache_api_url",
        "//server/endpoint_urls/events_api_url",
        "//server/endpoint_urls/remote_exec_api_url",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/bazel_request",
        "//server/util/db",
        "//server/util/git",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/rexec",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@in_gopkg_yaml_v2//:yaml_v2",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

go_test(
    name = "hostedrunner_test",
    srcs = ["hostedrunner_test.go"],
    embed = [":hostedrunner"],
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/workflow/service",
        "//proto:buildbuddy_service_go_proto",
        "//proto:git_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/buildbuddy_server",
        "//server/remote_cache/byte_stream_server",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)
