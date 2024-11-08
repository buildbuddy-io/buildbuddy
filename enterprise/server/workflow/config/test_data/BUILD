load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise/server/workflow/config:__subpackages__
package(default_visibility = [
    "//enterprise/server/workflow/config:__subpackages__",
])

go_library(
    name = "test_data",
    srcs = ["test_data.go"],
    embedsrcs = [
        "basic.yaml",
        "yaml_with_run_block.yaml",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config/test_data",
)
