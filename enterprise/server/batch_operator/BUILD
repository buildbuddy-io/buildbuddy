load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "batch_operator",
    srcs = ["batch_operator.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/log",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "batch_operator_test",
    size = "small",
    srcs = ["batch_operator_test.go"],
    deps = [
        ":batch_operator",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testhealthcheck",
        "//server/util/authutil",
        "//server/util/log",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
