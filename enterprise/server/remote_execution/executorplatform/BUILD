load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executorplatform",
    srcs = ["executorplatform.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executorplatform",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/status",
        "//server/util/usageutil",
    ],
)

go_test(
    name = "executorplatform_test",
    srcs = ["executorplatform_test.go"],
    embed = [":executorplatform"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/platform",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
