load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(srcs = ["vmexec.go"])

go_library(
    name = "vmexec",
    srcs = ["vmexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_elastic_gosigar//:gosigar",
        "@com_github_tklauser_go_sysconf//:go-sysconf",
        "@com_github_vishvananda_netlink//:netlink",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "vmexec_test",
    srcs = ["vmexec_test.go"],
    flaky = True,
    deps = [
        ":vmexec",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/vmexec_client",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
