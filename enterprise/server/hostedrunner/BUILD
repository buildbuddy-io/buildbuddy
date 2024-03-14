load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hostedrunner",
    srcs = ["hostedrunner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner",
    deps = [
        "//enterprise/server/cmd/ci_runner/bundle",
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:runner_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/endpoint_urls/cache_api_url",
        "//server/endpoint_urls/events_api_url",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
        "//server/util/git",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_exp//slices",
    ],
)

go_test(
    name = "hostedrunner_test",
    srcs = ["hostedrunner_test.go"],
    embed = [":hostedrunner"],
    deps = [
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
