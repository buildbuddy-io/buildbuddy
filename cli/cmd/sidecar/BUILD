load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "sidecar_lib",
    srcs = ["sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cmd/sidecar",
    visibility = ["//visibility:private"],
    deps = [
        "//cli/cache_proxy",
        "//cli/devnull",
        "//cli/logging",
        "//proto:publish_build_event_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:sidecar_go_proto",
        "//server/backends/disk_cache",
        "//server/build_event_protocol/build_event_proxy",
        "//server/build_event_protocol/build_event_server",
        "//server/nullauth",
        "//server/real_environment",
        "//server/rpc/filters",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//reflection",
    ],
)

go_binary(
    name = "sidecar",
    embed = [":sidecar_lib"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "sidecar-darwin-amd64",
    out = "sidecar-darwin-amd64",
    embed = [":sidecar_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "sidecar-darwin-arm64",
    out = "sidecar-darwin-arm64",
    embed = [":sidecar_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "sidecar-linux-amd64",
    out = "sidecar-linux-amd64",
    embed = [":sidecar_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "sidecar-linux-arm64",
    out = "sidecar-linux-arm64",
    embed = [":sidecar_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "sidecar-windows-amd64",
    out = "sidecar-windows-amd64.exe",
    embed = [":sidecar_lib"],
    goarch = "amd64",
    goos = "windows",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
