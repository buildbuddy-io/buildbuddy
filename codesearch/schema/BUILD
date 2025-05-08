load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "schema",
    srcs = ["schema.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/schema",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/token",
        "//codesearch/types",
        "//server/util/status",
    ],
)

go_test(
    name = "schema_test",
    srcs = ["schema_test.go"],
    deps = [
        ":schema",
        "//codesearch/token",
        "//codesearch/types",
    ],
)
