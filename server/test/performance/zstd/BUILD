load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "zstd_test",
    srcs = ["zstd_test.go"],
    args = [
        "-test.bench=.",
    ],
    deps = [
        "@com_github_klauspost_compress//zstd",
        "@com_github_stretchr_testify//require",
        "@com_github_valyala_gozstd//:gozstd",
    ],
)
