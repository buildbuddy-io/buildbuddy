load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":defs.bzl", "bazel_binary_targets", "bazel_pkg_tar")

ALL_BAZEL_VERSIONS = [
    "6.6.0",
    "7.7.1",
    "8.3.1",
    "8.4.2",
    "8.5.1",
    "8.6.0",
    "8.7.0",
    "9.0.2",
    "9.1.1",
]

SUPPORTED_BAZEL_CLIENT_VERSIONS = [
    "7.7.1",
    "8.7.0",
    "9.1.1",
]

# Keep currently deployed prober versions plus the popular Bazel 8 versions
# that phase 1 can switch configs to after the image is deployed.
PROBER_BAZEL_VERSIONS = [
    "8.3.1",
    "8.4.2",
    "8.5.1",
    "8.6.0",
]

bazel_binary_targets(
    versions = ALL_BAZEL_VERSIONS,
    visibility = ["//visibility:public"],
)

bazel_pkg_tar(
    name = "bazel_binaries_tar",
    versions = ALL_BAZEL_VERSIONS,
    visibility = ["//visibility:public"],
)

bazel_pkg_tar(
    name = "prober_bazel_binaries_tar",
    versions = PROBER_BAZEL_VERSIONS,
    visibility = ["//visibility:public"],
)

sh_test(
    name = "supported_bazel_clients_test",
    srcs = ["supported_bazel_clients_test.sh"],
    args = [
        "7.7.1=$(rootpath :bazel-7.7.1_crossplatform)",
        "8.7.0=$(rootpath :bazel-8.7.0_crossplatform)",
        "9.1.1=$(rootpath :bazel-9.1.1_crossplatform)",
    ],
    data = [
        ":bazel-7.7.1_crossplatform",
        ":bazel-8.7.0_crossplatform",
        ":bazel-9.1.1_crossplatform",
    ],
)

go_library(
    name = "bazel",
    srcs = ["bazel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)
