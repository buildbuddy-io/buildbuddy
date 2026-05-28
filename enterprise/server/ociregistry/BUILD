load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//:deps.bzl", "PODMAN_VERSION")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ociregistry",
    srcs = ["ociregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/ocicache",
        "//enterprise/server/util/trafficstats",
        "//proto:ociregistry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/http/httpclient",
        "//server/metrics",
        "//server/real_environment",
        "//server/util/clientip",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/prefix",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "ociregistry_test",
    timeout = "short",
    srcs = ["ociregistry_test.go"],
    deps = [
        ":ociregistry",
        "//enterprise/server/clientidentity",
        "//enterprise/server/testutil/testregistry",
        "//server/interfaces",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/random",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "oci_client_integration_test",
    size = "large",
    timeout = "moderate",
    srcs = ["oci_client_integration_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "podman-static@" + PODMAN_VERSION,
    },
    tags = ["docker"],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    deps = [
        "//enterprise/server/testutil/testregistry",
        "//enterprise/server/util/testpodman",
        "//server/testutil/testhttp",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
    ],
)
