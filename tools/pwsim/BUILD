load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "pwsim_test",
    srcs = ["pwsim_test.go"],
    deps = [
        "//server/util/consistent_hash",
        "//server/util/hash",
        "//server/util/uuid",
        "//server/util/wrand",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_stretchr_testify//require",
    ],
)
