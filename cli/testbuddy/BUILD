load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "testbuddy",
    srcs = ["testbuddy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testbuddy",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/flaghistory",
        "//cli/log",
        "//cli/login",
        "//cli/workspace",
        "//proto:test_buddy_go_proto",
        "//server/test_buddy/identity",
        "//server/test_buddy/junit",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "testbuddy_test",
    srcs = ["testbuddy_test.go"],
    deps = [
        ":testbuddy",
        "//proto:test_buddy_go_proto",
        "//server/test_buddy/junit",
        "//server/test_buddy/normalize",
        "@com_github_stretchr_testify//require",
    ],
)
