load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bb-crossplatform",
    srcs = ["//cli/cmd/bb"],
    outs = ["bb-bin"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bb",
    srcs = ["bb.go"],
    embedsrcs = ["bb-bin"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bb",
    visibility = ["//visibility:public"],
)
