load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "box",
    srcs = ["box.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/box",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/ssh",
        "//cli/version",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:firecracker_go_proto",
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/util/bazel_request",
        "//server/util/error_util",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/platform",
        "//server/util/retry",
        "//server/util/rexec",
        "//server/util/status",
        "//server/util/uuid",
        "@com_github_dustinkirkland_golang_petname//:golang-petname",
        "@land_charm_lipgloss_v2//:lipgloss",
        "@land_charm_lipgloss_v2//table",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_term//:term",
    ],
)

go_test(
    name = "box_test",
    srcs = ["box_test.go"],
    embed = [":box"],
    deps = [
        "//proto:gateway_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
