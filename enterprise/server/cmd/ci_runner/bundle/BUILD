load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(
    default_visibility = [
        "//enterprise:__subpackages__",
    ],
)

genrule(
    name = "embedsrcs_compatible_ci_runner",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["//enterprise/server/cmd/ci_runner:ci_runner_linux_x86_64_static"],
        "//platforms/configs:linux_arm64": ["//enterprise/server/cmd/ci_runner:ci_runner_linux_arm64_static"],
        "//conditions:default": ["//enterprise/server/cmd/ci_runner"],
    }),
    outs = ["buildbuddy_ci_runner"],
    cmd_bash = "cp $(SRCS) $@",
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = ["buildbuddy_ci_runner"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle",
)
