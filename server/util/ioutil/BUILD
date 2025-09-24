load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ioutil",
    srcs = ["ioutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/ioutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/bytebufferpool",
        "//server/util/status",
    ],
)

go_test(
    name = "ioutil_test",
    size = "small",
    srcs = ["ioutil_test.go"],
    deps = [
        ":ioutil",
        "//server/testutil/testdigest",
        "//server/util/bytebufferpool",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
    ],
)
