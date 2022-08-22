load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "status",
    srcs = ["status.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/status",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_pkg_errors//:errors",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "status_test",
    size = "small",
    srcs = ["status_test.go"],
    deps = [
        ":status",
        "@com_github_pkg_errors//:errors",
        "@com_github_stretchr_testify//assert",
    ],
)
