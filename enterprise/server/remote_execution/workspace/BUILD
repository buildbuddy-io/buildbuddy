load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "workspace",
    srcs = ["workspace.go"],
    data = ["//enterprise/server/cmd/ci_runner:buildbuddy_ci_runner"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace",
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//enterprise/server/remote_execution/vfs",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_gobwas_glob//:glob",
        "@com_github_google_uuid//:uuid",
        "@io_opentelemetry_go_otel//attribute",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "workspace_test",
    size = "small",
    srcs = ["workspace_test.go"],
    deps = [
        ":workspace",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
