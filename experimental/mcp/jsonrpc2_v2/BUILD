load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "jsonrpc2_v2",
    srcs = [
        "conn.go",
        "frame.go",
        "jsonrpc2.go",
        "messages.go",
        "net.go",
        "serve.go",
        "wire.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/jsonrpc2_v2",
    visibility = ["//visibility:public"],
)

go_test(
    name = "jsonrpc2_v2_test",
    srcs = [
        "jsonrpc2_test.go",
        "serve_test.go",
        "wire_test.go",
    ],
    deps = [
        "@org_golang_x_tools//internal/event/export/eventtest",
        "@org_golang_x_tools//internal/jsonrpc2_v2",
        "@org_golang_x_tools//internal/stack/stacktest",
        "@org_golang_x_tools//internal/testenv",
    ],
)
