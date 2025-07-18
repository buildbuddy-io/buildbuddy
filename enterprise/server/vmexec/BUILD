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
        "//enterprise/server/util/vsock",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/remote_cache/digest",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_elastic_gosigar//:gosigar",
        "@com_github_miekg_dns//:dns",
        "@com_github_tklauser_go_sysconf//:go-sysconf",
        "@com_github_vishvananda_netlink//:netlink",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "vmexec_test",
    srcs = ["vmexec_test.go"],
    exec_properties = {
        # Run in a firecracker VM since this test runs sync() and we don't want
        # to cause unnecessary IO on the host machine.
        "test.workload-isolation-type": "firecracker",
    },
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
        "@org_golang_google_grpc//:grpc",
    ],
)
