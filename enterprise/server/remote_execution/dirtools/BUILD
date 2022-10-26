load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = [
    "//cli/remotebazel:__pkg__",
    "//enterprise:__subpackages__",
])

go_library(
    name = "dirtools",
    srcs = ["dirtools.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/compression",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "dirtools_test",
    size = "small",
    srcs = ["dirtools_test.go"],
    deps = [
        ":dirtools",
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/hash",
        "//server/util/prefix",
        "@com_github_stretchr_testify//assert",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)
