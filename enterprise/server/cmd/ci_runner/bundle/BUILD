load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(
    default_visibility = [
        "//enterprise:__subpackages__",
    ],
)

copy_file(
    name = "embedsrcs_compatible_ci_runner",
    src = "//enterprise/server/cmd/ci_runner",
    out = "buildbuddy_ci_runner",
    allow_symlink = True,
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = ["buildbuddy_ci_runner"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle",
)
