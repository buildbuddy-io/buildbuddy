load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(
    default_visibility = [
        "//enterprise:__subpackages__",
    ],
)

genrule(
    name = "embedsrcs_compatible_ci_runner",
    srcs = ["//enterprise/server/cmd/ci_runner"],
    outs = ["buildbuddy_ci_runner"],
    cmd_bash = "cp $(SRCS) $@",
)

genrule(
    name = "empty_for_embedsrcs",
    outs = ["empty"],
    cmd_bash = "touch $@",
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = select({
        "//:fastbuild": [
            ":empty",  # embed fails with no embedsrcs
        ],
        "//conditions:default": [
            ":buildbuddy_ci_runner",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle",
    deps = [
        "//server/util/fileresolver",
    ],
)
