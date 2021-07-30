load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "firecracker",
    srcs = [
        "firecracker.go",
        "firecracker_darwin.go",
    ],
    data = [
        "//enterprise/vmsupport/bin:initrd.cpio",
        "//enterprise/vmsupport/bin:vmlinux",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//server/interfaces",
            "//server/util/status",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise:bundle",
            "//enterprise/server/remote_execution/commandutil",
            "//enterprise/server/remote_execution/container",
            "//enterprise/server/remote_execution/snaploader",
            "//enterprise/server/util/container",
            "//enterprise/server/util/ext4",
            "//enterprise/server/util/networking",
            "//enterprise/server/util/vsock",
            "//proto:remote_execution_go_proto",
            "//proto:vmexec_go_proto",
            "//server/environment",
            "//server/interfaces",
            "//server/util/disk",
            "//server/util/log",
            "//server/util/status",
            "@com_github_firecracker_microvm_firecracker_go_sdk//:firecracker-go-sdk",
            "@com_github_firecracker_microvm_firecracker_go_sdk//client/models",
            "@com_github_google_uuid//:uuid",
            "@com_github_sirupsen_logrus//:logrus",
            "@io_bazel_rules_go//go/tools/bazel:go_default_library",
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "firecracker_test",
    srcs = ["firecracker_test.go"],
    tags = ["manual"],  # Don't include this target in wildcard patterns
    deps = [
        ":firecracker",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/config",
        "//server/interfaces",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/disk",
        "@com_github_stretchr_testify//assert",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)
