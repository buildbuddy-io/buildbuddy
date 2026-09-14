load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "lint_lib",
    srcs = ["lint.go"],
    data = [
        "//tools/bb",
        "//tools/prettier",
        "@io_bazel_rules_go//go",
        "@llvm//tools:clang-format",
        "@org_golang_x_tools//cmd/goimports",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/lint",
    visibility = ["//visibility:private"],
    x_defs = {
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

# keep: Compile the source directly so unit tests do not build the real lint tools
# included in lint_lib's runfiles.
go_test(
    name = "lint_test",
    srcs = [
        "lint.go",
        "lint_test.go",
    ],
    deps = [
        "//server/util/flag",
        "//server/util/ioutil",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "@com_github_rs_zerolog//:zerolog",
        "@com_github_rs_zerolog//log",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_x_sync//errgroup",
    ],
)
