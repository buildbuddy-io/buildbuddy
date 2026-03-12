load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "expiring_lru",
    srcs = ["expiring_lru.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/expiring_lru",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/lru",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "expiring_lru_test",
    size = "small",
    srcs = ["expiring_lru_test.go"],
    deps = [
        ":expiring_lru",
        "//server/util/lru",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
