load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hostedrunner",
    srcs = ["hostedrunner.go"],
    data = ["//enterprise/server/cmd/ci_runner:buildbuddy_ci_runner"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner",
    deps = [
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
        "//server/tables",
        "//server/util/bazel_request",
        "//server/util/git",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//status",
    ],
)
