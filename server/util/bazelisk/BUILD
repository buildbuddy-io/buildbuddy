load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bazelisk-binary",
    srcs = ["@com_github_bazelbuild_bazelisk//:bazelisk"],
    outs = ["bazelisk-bin"],
    cmd = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazelisk",
    srcs = ["bazelisk.go"],
    embedsrcs = [":bazelisk-bin"],  #keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazelisk",
    visibility = ["//visibility:public"],
)
