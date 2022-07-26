load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "approximatelru",
    srcs = ["approximatelru.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/approximatelru",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/hash",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cespare_xxhash_v2//:xxhash",
    ],
)

go_test(
    name = "approximatelru_test",
    size = "small",
    srcs = ["approximatelru_test.go"],
    deps = [
        ":approximatelru",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "@com_github_stretchr_testify//require",
    ],
)
