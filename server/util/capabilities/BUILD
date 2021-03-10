load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["capabilities.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/capabilities",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment:go_default_library",
        "//server/util/status:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

# NOTE: Can't embed ":go_default_library" because the test depends on
# "//server/testutil/auth:go_default_library" which depends on ":go_default_library".
#
# Due to https://github.com/bazelbuild/rules_go/issues/2808 tests can't have indirect
# deps on the embedded lib.
#
# So instead of the usual embedding approach, we use a normal dep here.

go_test(
    name = "go_default_test",
    srcs = ["capabilities_test.go"],
    embed = [],  # keep
    deps = [
        ":go_default_library",  # keep
        "//proto:api_key_go_proto",
        "//server/interfaces:go_default_library",
        "//server/nullauth:go_default_library",
        "//server/testutil/auth:go_default_library",
        "//server/testutil/environment:go_default_library",
        "//server/util/testing/flags:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)
