load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vmexec",
    srcs = ["vmexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_vishvananda_netlink//:netlink",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "vmexec_test",
    srcs = ["vmexec_test.go"],
    deps = [
        ":vmexec",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/vmexec_client",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
