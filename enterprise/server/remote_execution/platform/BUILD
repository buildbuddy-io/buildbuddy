load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "platform",
    srcs = ["platform.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "platform_test",
    size = "small",
    srcs = ["platform_test.go"],
    embed = [":platform"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
