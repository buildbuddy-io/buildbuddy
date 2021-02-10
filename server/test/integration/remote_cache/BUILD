load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["remote_cache_test.go"],
    # TODO: Make this test automatic instead of manual. We can either:
    # - Migrate the dependent Bazel binary to be cross-platform (e.g. build from source), or
    # - Migrate BB to Bazel 4.0.0 and set "target_compatible_with" on this test,
    #   so that it only gets run on Linux.
    tags = ["manual"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//server/buildbuddy_server:go_default_library",
        "//server/testutil/auth:go_default_library",
        "//server/testutil/bazel:go_default_library",
        "//server/testutil/environment:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
