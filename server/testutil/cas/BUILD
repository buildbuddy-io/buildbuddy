load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cas",
    testonly = 1,
    srcs = ["cas.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/cas",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/testutil/testdigest",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
