load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load("//dockerfiles/test_images:defs.bzl", "NET_TOOLS_IMAGE")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ociruntime",
    srcs = ["ociruntime.go"],
    data = [":crun"],
    embedsrcs = [
        "hosts",
        "seccomp.json",
        "tini",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
    x_defs = {
        "crunRlocationpath": "$(rlocationpath :crun)",
    },
    deps = [
        "//enterprise/server/remote_execution/block_io",
        "//enterprise/server/remote_execution/cgroup",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/executor_auth",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/ociconv",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/claims",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/fsync",
        "//server/util/hash",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/platform",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/statusz",
        "//server/util/unixcred",
        "//server/util/uuid",
        "//third_party/singleflight",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@io_bazel_rules_go//go/runfiles",
        "@io_cncf_tags_container_device_interface//pkg/cdi",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

genrule(
    name = "tini_binary",
    srcs = ["//enterprise/server/cmd/executor:tini"],
    outs = ["tini"],
    cmd = "cp $(SRCS) $@",
)

go_test(
    name = "ociruntime_test",
    srcs = ["ociruntime_test.go"],
    data = [
        ":crun",
        "//enterprise/server/cmd/executor:tini",
        "//enterprise/server/remote_execution/runner/testworker",
        "@busybox",
        "@net_tools",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://" + NET_TOOLS_IMAGE,
        "test.EstimatedComputeUnits": "2",
        "test.EstimatedFreeDiskBytes": "10GB",
    },
    # NOTE: if testing locally, use --test_sharding_strategy=disabled to work
    # around the networking package not supporting cross-process locks.
    shard_count = 4,
    tags = [
        "docker",
        "no-sandbox",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        # TODO: GitHub Actions arm runners are running into cgroup-related issues;
        # fix and re-enable on arm64.
        "@platforms//cpu:x86_64",
    ],
    x_defs = {
        "crunRlocationpath": "$(rlocationpath :crun)",
        "busyboxImageRlocationpath": "$(rlocationpath @busybox)",
        "netToolsImageRlocationpath": "$(rlocationpath @net_tools)",
        "testworkerRlocationpath": "$(rlocationpath //enterprise/server/remote_execution/runner/testworker)",
    },
    deps = [
        ":ociruntime",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/persistentworker",
        "//enterprise/server/remote_execution/workspace",
        "//enterprise/server/testutil/testregistry",
        "//enterprise/server/util/cpuset",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:worker_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/testutil/quarantine",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testnetworking",
        "//server/testutil/testshell",
        "//server/testutil/testtar",
        "//server/util/disk",
        "//server/util/networking",
        "//server/util/platform",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_google_go_containerregistry//pkg/crane",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/empty",
        "@com_github_google_go_containerregistry//pkg/v1/mutate",
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

# Manual test for NVIDIA GPU support.
#
# From a Linux machine with an NVIDIA GPU:
# - Install `nvidia-container-toolkit` (need `nvidia-ctk` in PATH)
# - Run `sudo nvidia-ctk cdi generate --output=/var/run/cdi/nvidia.yaml` which
#   creates a CDI spec.
# - Run the executor with OCI runtime enabled and --executor.oci.cdi_devices=nvidia.com/gpu=all
# - Run this test against the executor with bazel, e.g. --config=local-rbe-linux
sh_test(
    name = "nvidia_gpu_test",
    srcs = ["nvidia_gpu_test.sh"],
    exec_properties = {
        "test.workload-isolation-type": "oci",
        "test.container-image": "docker://nvidia/cuda:12.4.1-devel-ubuntu22.04",
    },
    tags = ["manual"],
    target_compatible_with = ["@platforms//os:linux"],
)

alias(
    name = "crun",
    actual = select({
        "@platforms//cpu:x86_64": "@com_github_containers_crun_crun-linux-amd64//file:crun",
        "@platforms//cpu:aarch64": "@com_github_containers_crun_crun-linux-arm64//file:crun",
    }),
)

alias(
    name = "busybox",
    actual = select({
        "@platforms//cpu:x86_64": "@net_busybox_busybox-linux-amd64//file:busybox",
        "@platforms//cpu:aarch64": "@net_busybox_busybox-linux-arm64//file:busybox",
    }),
)
