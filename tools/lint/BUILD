load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

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
        "goimportsRlocationpath": "$(rlocationpath @org_golang_x_tools//cmd/goimports)",
        "goRlocationpath": "$(rlocationpath @io_bazel_rules_go//go)",
        "clangFormatRlocationpath": "$(rlocationpath @llvm//tools:clang-format)",
        "prettierRlocationpath": "$(rlocationpath //tools/prettier)",
        "bbCLIRlocationpath": "$(rlocationpath //tools/bb)",
        "gazelleRlocationpaths": "$(rlocationpaths :gazelle)",
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

# The gazelle target (a `gazelle` rule) that BuildDepsFix runs. Lint runs it from
# runfiles so that it's built by the same invocation as lint (with the same
# flags, e.g. --config=remote), rather than by a nested `bazel run //:gazelle`,
# which would build it locally and discard bazel's analysis cache.
#
# There's no default: lint fails if this isn't set (unless BuildDepsFix is
# excluded). Repos set it in their .bazelrc to their own gazelle target, e.g.
#   common --@com_github_buildbuddy_io_buildbuddy//tools/lint:gazelle=//:gazelle
label_flag(
    name = "gazelle",
    build_setting_default = ":no_gazelle",
)

# Sentinel meaning "not set". ($(rlocationpaths) rejects empty targets.)
write_file(
    name = "no_gazelle",
    out = "no_gazelle.txt",
    content = [],
)
