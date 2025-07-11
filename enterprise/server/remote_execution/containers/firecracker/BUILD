load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "firecracker",
    srcs = [
        "containeropts.go",
        "firecracker.go",
    ],
    data = select({
        "//platforms/configs:linux_x86_64": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
            "//enterprise/vmsupport/bin:vmlinux_6.1",
        ],
        "//platforms/configs:linux_arm64": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
        ],
    }),
    embedsrcs = ["guest_api_hash.sha256"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    x_defs = select({
        "//platforms/configs:linux_x86_64": {
            "initrdRunfilePath": "$(rlocationpath //enterprise/vmsupport/bin:initrd.cpio)",
            "vmlinuxRunfilePath": "$(rlocationpath //enterprise/vmsupport/bin:vmlinux)",
            "vmlinux6_1RunfilePath": "$(rlocationpath //enterprise/vmsupport/bin:vmlinux_6.1)",
        },
        "//platforms/configs:linux_arm64": {
            "initrdRunfilePath": "$(rlocationpath //enterprise/vmsupport/bin:initrd.cpio)",
            "vmlinuxRunfilePath": "$(rlocationpath //enterprise/vmsupport/bin:vmlinux)",
        },
    }),
    deps = [
        "//enterprise/server/remote_execution/block_io",
        "//enterprise/server/remote_execution/cgroup",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/remote_execution/uffd",
        "//enterprise/server/remote_execution/vbd",
        "//enterprise/server/remote_execution/vmexec_client",
        "//enterprise/server/util/cpuset",
        "//enterprise/server/util/ext4",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/ociconv",
        "//enterprise/server/util/vfs_server",
        "//enterprise/server/util/vsock",
        "//enterprise/vmsupport:bundle",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:vmexec_go_proto",
        "//proto:vmvfs_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/error_util",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/uuid",
        "@com_github_armon_circbuf//:circbuf",
        "@com_github_firecracker_microvm_firecracker_go_sdk//:firecracker-go-sdk",
        "@com_github_firecracker_microvm_firecracker_go_sdk//client/models",
        "@com_github_firecracker_microvm_firecracker_go_sdk//client/operations",
        "@com_github_klauspost_cpuid_v2//:cpuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_sirupsen_logrus//:logrus",
        "@io_bazel_rules_go//go/runfiles",
        "@io_opentelemetry_go_otel//attribute",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_google_protobuf//types/known/timestamppb",
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
    cmd_bash = """
    if type sha256sum &>/dev/null; then
      # Linux
      sha256sum $(SRCS) | sha256sum | awk '{printf "%s", $$1}' > $@
    else
      # macOS
      shasum -a 256 $(SRCS) | shasum -a 256 | awk '{printf "%s", $$1}' > $@
    fi
    """,
    target_compatible_with = [
        "@platforms//os:linux",
    ],
)

# NOTE: to run this test locally, use `test.sh` or `bench.sh`
#
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
    args = [
        "--executor.enable_local_snapshot_sharing=true",
        "--executor.enable_remote_snapshot_sharing=true",
        "--executor.firecracker_enable_linux_6_1=true",
    ],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
        # These tests can get pretty expensive when running on a cold executor
        # because of OCI image conversion. Size the task manually to avoid OOM.
        # TODO: include test ext4 images as deps to avoid conversion.
        "test.EstimatedComputeUnits": "10",
    },
    shard_count = 30,
    tags = [
        "bare",  # Firecracker tests must be run with bare execution so they aren't nested within another container
        "no-sandbox",  # Firecracker is not compatible with Bazel's sandbox environment
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":firecracker",
        "//enterprise/server/action_cache_server_proxy",
        "//enterprise/server/byte_stream_server_proxy",
        "//enterprise/server/clientidentity",
        "//enterprise/server/ociregistry",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/remote_execution/workspace",
        "//enterprise/server/testutil/testcontainer",
        "//enterprise/server/util/cpuset",
        "//enterprise/server/util/ext4",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/resources",
        "//server/rpc/interceptors",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testmetrics",
        "//server/testutil/testnetworking",
        "//server/testutil/testport",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/random",
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
