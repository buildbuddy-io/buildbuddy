load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "worker_lib",
    srcs = ["worker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/buildgrep/worker",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//server/util/authutil",
        "//server/util/grpc_client",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "buildgrep_worker_linux_amd64",
    out = "buildgrep_worker",
    embed = [":worker_lib"],
    goarch = "amd64",
    goos = "linux",
    pure = "on",
)

go_test(
    name = "worker_test",
    srcs = ["worker_test.go"],
    embed = [":worker_lib"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
