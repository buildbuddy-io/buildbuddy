load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "vfs_server",
    srcs = [
        "vfs_server.go",
        "vfs_server_darwin.go",
        "vfs_server_linux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server",
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//proto:vfs_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/status",
        "@com_github_hanwen_go_fuse_v2//fs",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "vfs_server_test",
    size = "small",
    srcs = ["vfs_server_test.go"],
    deps = [
        ":vfs_server",
        "//proto:vfs_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
