load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "usage_test",
    srcs = ["usage_test.go"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:usage_go_proto",
        "//server/tables",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
