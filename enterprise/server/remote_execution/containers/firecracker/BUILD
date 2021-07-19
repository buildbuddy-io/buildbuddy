load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

go_library(
    name = "firecracker",
    srcs = ["firecracker.go"],
    data = [
        "//enterprise/vmsupport/bin:initrd.cpio",
        "//enterprise/vmsupport/bin:vmlinux",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise:bundle",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
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
)

go_test(
    name = "firecracker_test",
    srcs = ["firecracker_test.go"],
    tags = ["manual"],  # Don't include this target in wildcard patterns
    deps = [
        ":firecracker",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/disk",
        "@com_github_stretchr_testify//assert",
    ],
)

container_image(
    name = "base_image",
    base = "@executor_image//image:dockerfile_image.tar",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "firecracker_test_image",
    testonly = 1,
    base = ":base_image",
    binary = ":firecracker_test",
    visibility = ["//visibility:public"],
)
