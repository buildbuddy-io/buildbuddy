load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "podman",
    srcs = ["podman.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
    ],
)

go_test(
    name = "podman_test",
    srcs = ["podman_test.go"],
    tags = [
        "manual",
        "no-sandbox",
    ],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
    ],
)
