load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "podman",
    srcs = ["podman.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman",
    deps = [
        "//enterprise/server/remote_execution/cgroup",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/soci_store",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/networking",
        "//server/util/random",
        "//server/util/retry",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_masterminds_semver_v3//:semver",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "podman_test",
    srcs = ["podman_test.go"],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
