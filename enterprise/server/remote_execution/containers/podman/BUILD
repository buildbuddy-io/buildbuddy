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
        "@com_github_masterminds_semver_v3//:semver",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

# To run this test with remote execution, add a test_tag_filter:
# bazel test --config=remote --test_tag_filters=+bare \
# //enterprise/server/remote_execution/containers/podman:podman_test
go_test(
    name = "podman_integration_test",
    srcs = ["podman_integration_test.go"],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
    },
    tags = [
        "bare",  # Podman tests must be run with bare execution so they aren't nested within another container
        "no-sandbox",  # Firecracker is not compatible with Bazel's sandbox environment
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "podman_test",
    srcs = ["podman_integration_test.go"],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
