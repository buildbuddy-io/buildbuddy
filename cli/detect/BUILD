load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "detect",
    srcs = [
        "detect.go",
        "flake.go",
        "nondeterminism.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/detect",
    deps = [
        "//cli/arg",
        "//cli/explain",
        "//cli/log",
        "//cli/login",
        "//cli/parser/bazelrc",
        "//cli/remotebazel",
        "//proto:buildbuddy_service_go_proto",
        "//proto:command_line_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:git_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:notification_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//proto:spawn_diff_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/shlex",
        "//server/util/status",
        "//server/util/uuid",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "detect_test",
    srcs = [
        "flake_test.go",
        "nondeterminism_test.go",
    ],
    embed = [":detect"],
    deps = [
        "//cli/arg",
        "//cli/parser",
        "//cli/parser/test_data",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "//proto:spawn_diff_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
