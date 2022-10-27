load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "podman",
    srcs = ["podman.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/networking",
        "//server/util/perms",
        "//server/util/random",
        "//server/util/retry",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "podman_test",
    srcs = ["podman_test.go"],
    args = [
        # Note: these are used only for the Stats tests, which require running as root.
        # These paths were tested on Ubuntu 20.04 + podman 3.4.2, but may be different for other setups.
        # See the comment in podman.go for more info.
        "--executor.podman.memory_usage_path_template=/sys/fs/cgroup/memory/machine.slice/libpod-{{.ContainerID}}.scope/memory.usage_in_bytes",
        "--executor.podman.cpu_usage_path_template=/sys/fs/cgroup/cpu,cpuacct/machine.slice/libpod-{{.ContainerID}}.scope/cpuacct.usage",
    ],
    tags = [
        "manual",
        "no-sandbox",
    ],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
