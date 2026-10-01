load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

# The gazelle runner (a `gazelle` rule target) used to update BUILD files.
# Repos that run this linter as an external repo should point this at their
# own gazelle target, e.g. in .bazelrc:
#
#   common --@com_github_buildbuddy_io_buildbuddy//tools/lint:gazelle=//:gazelle
label_flag(
    name = "gazelle",
    build_setting_default = "//:gazelle",
)

go_library(
    name = "lint_lib",
    srcs = ["lint.go"],
    data = [
        ":gazelle",
        "//tools/bb",
        "//tools/prettier",
        "@com_github_bazel_contrib_buildtools_v10//buildifier",
        "@io_bazel_rules_go//go",
        "@llvm//tools:clang-format",
        "@org_golang_x_tools//cmd/goimports",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/lint",
    visibility = ["//visibility:private"],
    x_defs = {
        "buildifierRlocationpath": "$(rlocationpath @com_github_bazel_contrib_buildtools_v10//buildifier)",
        "gazelleRlocationpath": "$(rlocationpath :gazelle)",
        "goimportsRlocationpath": "$(rlocationpath @org_golang_x_tools//cmd/goimports)",
        "goRlocationpath": "$(rlocationpath @io_bazel_rules_go//go)",
        "clangFormatRlocationpath": "$(rlocationpath @llvm//tools:clang-format)",
        "prettierRlocationpath": "$(rlocationpath //tools/prettier)",
        "bbCLIRlocationpath": "$(rlocationpath //tools/bb)",
    },
    deps = [
        "//server/util/flag",
        "//server/util/ioutil",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "lint",
    embed = [":lint_lib"],
    visibility = ["//visibility:public"],
)
