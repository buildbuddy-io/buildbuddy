load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cow_cgo_testutil",
    testonly = 1,
    srcs = ["cow_cgo_testutil.go"],
    cgo = True,
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write/cow_cgo_testutil",
    visibility = ["//visibility:public"],
)
