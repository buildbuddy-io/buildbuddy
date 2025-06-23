load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "jsonrpc2",
    srcs = [
        "conn.go",
        "frame.go",
        "jsonrpc2.go",
        "messages.go",
        "net.go",
        "serve.go",
        "wire.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/internal/jsonrpc2",
    visibility = ["//experimental/modelcontextprotocol/go-sdk:__subpackages__"],
)

go_test(
    name = "jsonrpc2_test",
    srcs = [
        "jsonrpc2_test.go",
        "serve_test.go",
        "wire_test.go",
    ],
    deps = [":jsonrpc2"],
)
