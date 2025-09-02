load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "gormutil",
    srcs = ["gormutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/gormutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//logger",
        "@io_gorm_gorm//utils",
    ],
)

go_test(
    name = "gormutil_test",
    srcs = ["gormutil_test.go"],
    deps = [
        ":gormutil",
        "@com_github_rs_zerolog//log",
        "@com_github_stretchr_testify//require",
    ],
)
