load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "lint_lib",
    srcs = ["lint.go"],
    data = [
        "//:node_modules/prettier-plugin-organize-imports",
        "//:node_modules/prettier-plugin-organize-imports/dir",
        "//:node_modules/stylelint-value-no-unknown-custom-properties",
        "//cli/cmd/bb",
        "//tools/clang-format",
        "//tools/prettier",
        "//tools/stylelint",
        "@io_bazel_rules_go//go",
        "@org_golang_x_tools//cmd/goimports",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/lint",
    visibility = ["//visibility:private"],
    x_defs = {
        "goimportsRlocationpath": "$(rlocationpath @org_golang_x_tools//cmd/goimports)",
        "goRlocationpath": "$(rlocationpath @io_bazel_rules_go//go)",
        "clangFormatRlocationpath": "$(rlocationpath //tools/clang-format)",
        "prettierRlocationpath": "$(rlocationpath //tools/prettier)",
        "prettierPluginOrganizeImportsRlocationpath": "$(rlocationpath //:node_modules/prettier-plugin-organize-imports/dir)",
        "bbCLIRlocationpath": "$(rlocationpath //cli/cmd/bb)",
        "stylelintRlocationpath": "$(rlocationpath //tools/stylelint)",
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
