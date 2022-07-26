load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "remote_cache_test",
    size = "small",
    srcs = ["remote_cache_test.go"],
    shard_count = 6,
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:cache_go_proto",
        "//proto:invocation_go_proto",
        "//server/testutil/testbazel",
        "//server/util/retry",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
