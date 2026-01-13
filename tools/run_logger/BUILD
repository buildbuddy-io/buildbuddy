load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "run_logger_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/run_logger",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_creack_pty//:pty",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata:go_default_library",
    ],
)

go_binary(
    name = "run_logger",
    embed = [":run_logger_lib"],
    visibility = ["//visibility:public"],
)

sh_binary(
    name = "run_logger_wrapper",
    srcs = ["wrapper.sh"],
    data = [":run_logger"],
    visibility = ["//visibility:public"],
)

# Test binary
go_binary(
    name = "hello",
    srcs = ["testdata/hello.go"],
    visibility = ["//visibility:public"],
)
