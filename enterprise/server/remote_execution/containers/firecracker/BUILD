load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//tools/vmstart:__pkg__",
])

go_library(
    name = "firecracker",
    srcs = [
        "containeropts.go",
        "firecracker.go",
        "firecracker_darwin.go",
    ],
    data = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
        ],
        "//conditions:default": [],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker",
    deps = [
        "@com_github_docker_docker//client",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//server/environment",
            "//server/interfaces",
            "//server/util/status",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/commandutil",
            "//enterprise/server/remote_execution/container",
            "//enterprise/server/remote_execution/snaploader",
            "//enterprise/server/remote_execution/vmexec_client",
            "//enterprise/server/util/container",
            "//enterprise/server/util/ext4",
            "//enterprise/server/util/vfs_server",
            "//enterprise/server/util/vsock",
            "//proto:remote_execution_go_proto",
            "//proto:vmexec_go_proto",
            "//proto:vmvfs_go_proto",
            "//server/environment",
            "//server/interfaces",
            "//server/util/alert",
            "//server/util/background",
            "//server/util/disk",
            "//server/util/hash",
            "//server/util/log",
            "//server/util/networking",
            "//server/util/status",
            "//server/util/tracing",
            "@com_github_armon_circbuf//:circbuf",
            "@com_github_firecracker_microvm_firecracker_go_sdk//:firecracker-go-sdk",
            "@com_github_firecracker_microvm_firecracker_go_sdk//client/models",
            "@com_github_firecracker_microvm_firecracker_go_sdk//client/operations",
            "@com_github_google_uuid//:uuid",
            "@com_github_sirupsen_logrus//:logrus",
            "@org_golang_google_grpc//:go_default_library",
            "@org_golang_x_sync//errgroup",
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "firecracker_test",
    timeout = "long",
    srcs = ["firecracker_test.go"],
    tags = [
        "manual",  # Don't include this target in wildcard patterns
        "no-sandbox",  # Firecracker is not compatible with Bazel's sandbox environment
    ],
    deps = [
        ":firecracker",
        "//enterprise:bundle",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/workspace",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/interfaces",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/fileresolver",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)
