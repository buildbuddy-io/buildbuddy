load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:default_visibility //enterprise:__subpackages__,@buildbuddy_internal//:__subpackages__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "@buildbuddy_internal//:__subpackages__",
])

go_library(
    name = "vfs_server",
    srcs = [
        "vfs_server.go",
        "vfs_server_unsupported.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server",
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//proto:vfs_go_proto",
            "//server/environment",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//proto:vfs_go_proto",
            "//server/cache/dirtools",
            "//server/environment",
            "//server/interfaces",
            "//server/remote_cache/cachetools",
            "//server/remote_cache/digest",
            "//server/util/alert",
            "//server/util/claims",
            "//server/util/grpc_server",
            "//server/util/log",
            "//server/util/proto",
            "//server/util/random",
            "//server/util/status",
            "//third_party/singleflight",
            "@com_github_hanwen_go_fuse_v2//fs",
            "@com_github_hanwen_go_fuse_v2//fuse",
            "@org_golang_google_grpc//:grpc",
            "@org_golang_google_grpc//codes",
            "@org_golang_google_grpc//status",
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//proto:vfs_go_proto",
            "//server/environment",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "vfs_server_test",
    size = "small",
    srcs = ["vfs_server_test.go"],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":vfs_server",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:vfs_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/hash",
        "//server/util/prefix",
        "@com_github_google_go_cmp//cmp",
        "@com_github_hanwen_go_fuse_v2//fuse",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
