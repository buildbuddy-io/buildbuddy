package(default_visibility = ["//visibility:public"])

load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/tryflame/buildbuddy
gazelle(name = "gazelle")

exports_files([
    "tsconfig.json",
    "package.json",
])

filegroup(
    name = "config_files",
    srcs = glob([
        "config/**",
    ]),
)
