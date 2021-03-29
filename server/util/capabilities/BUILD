load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "capabilities",
    srcs = ["capabilities.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/capabilities",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/util/status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
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
        "//server/testutil/testauth:go_default_library",
        "//server/testutil/testenv:go_default_library",
        "//server/util/testing/flags:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

go_test(
    name = "capabilities_test",
    srcs = ["capabilities_test.go"],
    deps = [
        ":capabilities",
        "//proto:api_key_go_proto",
        "//server/interfaces",
        "//server/nullauth",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
    ],
)
