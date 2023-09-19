load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "snaploader_utils",
    srcs = ["snaploader_utils.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader_utils",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "snaploader_utils_test",
    srcs = ["snaploader_utils_test.go"],
    deps = [
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/snaploader",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/random",
        "@com_github_stretchr_testify//require",
    ],
)
