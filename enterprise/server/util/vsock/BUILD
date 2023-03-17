load("@io_bazel_rules_go//go:def.bzl", "go_library")

# TODO(sluongng): ensure all //enterprise code are not dependency of FOSS code
# gazelle:default_visibility //enterprise:__subpackages__,//tools/vmexec_client:__pkg__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "//tools/vmexec_client:__pkg__",
])

go_library(
    name = "vsock",
    srcs = ["vsock.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock",
    deps = [
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_mdlayher_vsock//:vsock",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//backoff",
        "@org_golang_x_sys//unix",
    ],
)
