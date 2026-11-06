load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tunhelperutil",
    srcs = [
        "tunhelperutil.go",
        "tunhelperutil_darwin.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "tunhelperutil_test",
    srcs = [
        "tunhelperutil_darwin_test.go",
        "tunhelperutil_test.go",
    ],
    embed = [":tunhelperutil"],
    deps = ["@com_github_stretchr_testify//require"],
)
