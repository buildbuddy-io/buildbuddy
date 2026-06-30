load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vmexec_client",
    srcs = ["vmexec_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/log",
        "//server/util/rpcutil",
        "//server/util/status",
        "//server/util/tracing",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "vmexec_client_test",
    srcs = ["vmexec_client_test.go"],
    deps = [
        ":vmexec_client",
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/interfaces",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
