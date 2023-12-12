load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "firecracker",
    srcs = [
        "containeropts.go",
        "firecracker.go",
    ],
    data = [
        "//enterprise/vmsupport/bin:initrd.cpio",
        "//enterprise/vmsupport/bin:vmlinux",
    ],
    embedsrcs = ["guest_api_hash.sha256"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/remote_execution/uffd",
        "//enterprise/server/remote_execution/vbd",
        "//enterprise/server/remote_execution/vmexec_client",
        "//enterprise/server/util/ext4",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/ociconv",
        "//enterprise/server/util/vfs_server",
        "//enterprise/server/util/vsock",
        "//enterprise/vmsupport:bundle",
        "//proto:firecracker_go_proto",
        "//proto:health_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//proto:vmexec_go_proto",
        "//proto:vmvfs_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/perms",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_armon_circbuf//:circbuf",
        "@com_github_docker_docker//client",
        "@com_github_firecracker_microvm_firecracker_go_sdk//:firecracker-go-sdk",
        "@com_github_firecracker_microvm_firecracker_go_sdk//client/models",
        "@com_github_firecracker_microvm_firecracker_go_sdk//client/operations",
        "@com_github_google_uuid//:uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_sirupsen_logrus//:logrus",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

genrule(
    name = "guest_api_hash",
    srcs = [
        "//enterprise/server/cmd/goinit:main.go",
        "//enterprise/server/vmexec:vmexec.go",
    ],
    outs = ["guest_api_hash.sha256"],
    cmd_bash = """sha256sum $(SRCS) | sha256sum | awk '{printf "%s", $$1}' > $@""",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
)

# To remotely execute this test, a couple of tag_filters are needed:
# bazel test --config=remote --test_tag_filters=+bare \
# //enterprise/server/remote_execution/containers/firecracker:firecracker_test
go_test(
    name = "firecracker_test",
    timeout = "long",
    srcs = [
        "firecracker_performance_test.go",
        "firecracker_test.go",
    ],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
    },
    tags = [
        "bare",  # Firecracker tests must be run with bare execution so they aren't nested within another container
        "no-sandbox",  # Firecracker is not compatible with Bazel's sandbox environment
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":firecracker",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/runner",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/remote_execution/vbd",
        "//enterprise/server/remote_execution/workspace",
        "//enterprise/server/testutil/testcontainer",
        "//enterprise/server/util/ext4",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/backends/disk_cache",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/resources",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testmetrics",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/tracing",
        "@com_github_google_go_cmp//cmp",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_x_sync//errgroup",
    ],
)

# NOTE: to run this test locally, use `test.sh` or `bench.sh`
#
# TODO(bduffany): once blockio is fully enabled, set blockio flag(s) on
# firecracker_test and remove this one.
go_test(
    name = "firecracker_test_blockio",
    timeout = "long",
    srcs = [
        "firecracker_performance_test.go",
        "firecracker_test.go",
    ],
    args = [
        "--executor.firecracker_enable_vbd=true",
        "--executor.firecracker_enable_merged_rootfs=true",
        "--executor.firecracker_enable_uffd=true",
        "--executor.enable_local_snapshot_sharing=true",
        #        "--executor.enable_remote_snapshot_sharing=true", # TODO: Re-enable once performance has improved
    ],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
    },
    tags = [
        "bare",  # Firecracker tests must be run with bare execution so they aren't nested within another container
        "manual",  # TODO: Re-enable once tests are stable
        "no-sandbox",  # Firecracker is not compatible with Bazel's sandbox environment
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":firecracker",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/runner",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/remote_execution/vbd",
        "//enterprise/server/remote_execution/workspace",
        "//enterprise/server/testutil/testcontainer",
        "//enterprise/server/util/ext4",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/backends/disk_cache",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/resources",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testmetrics",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/tracing",
        "@com_github_google_go_cmp//cmp",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_x_sync//errgroup",
    ],
)
