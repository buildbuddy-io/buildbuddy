load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "remote_cache_test",
    srcs = ["remote_cache_test.go"],
    shard_count = 4,
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/backends/userdb",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testbazel",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
