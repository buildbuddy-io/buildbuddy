load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "fastcopy",
    srcs = [
        "fastcopy.go",
        "fastcopy_darwin.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fastcopy",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "fastcopy_test",
    srcs = ["fastcopy_test.go"],
    deps = [
        ":fastcopy",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
