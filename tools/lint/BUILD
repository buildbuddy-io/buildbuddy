load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "lint_lib",
    srcs = ["lint.go"],
    data = [
        "//:gazelle",
        "//server/util/bazelisk:bazelisk-crossplatform",
        "//tools/prettier",
        "@com_github_bazelbuild_buildtools//buildifier",
        "@io_bazel_rules_go//go",
        "@llvm//tools:clang-format",
        "@org_golang_x_tools//cmd/goimports",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/lint",
    visibility = ["//visibility:private"],
    x_defs = {
        "bazeliskRlocationpath": "$(rlocationpath //server/util/bazelisk:bazelisk-crossplatform)",
        "buildifierRlocationpath": "$(rlocationpath @com_github_bazelbuild_buildtools//buildifier)",
        "clangFormatRlocationpath": "$(rlocationpath @llvm//tools:clang-format)",
        "gazelleRlocationpath": "$(rlocationpath //:gazelle)",
        "goimportsRlocationpath": "$(rlocationpath @org_golang_x_tools//cmd/goimports)",
        "goRlocationpath": "$(rlocationpath @io_bazel_rules_go//go)",
        "prettierRlocationpath": "$(rlocationpath //tools/prettier)",
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

go_test(
    name = "lint_test",
    size = "small",
    srcs = ["lint_test.go"],
    embed = [":lint_lib"],
    deps = ["@com_github_stretchr_testify//require"],
)
