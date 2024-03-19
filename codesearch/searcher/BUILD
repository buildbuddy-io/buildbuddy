load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "searcher",
    srcs = ["searcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/searcher",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/result",
        "//codesearch/types",
        "//server/util/log",
        "//server/util/status",
        "@com_github_xiam_sexpr//ast",
        "@com_github_xiam_sexpr//parser",
        "@org_golang_x_exp//slices",
    ],
)
