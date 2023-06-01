load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "lockmap",
    srcs = ["lockmap.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/lockmap",
    visibility = ["//visibility:public"],
)

go_test(
    name = "lockmap_test",
    srcs = ["lockmap_test.go"],
    deps = [
        ":lockmap",
        "//server/util/random",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
