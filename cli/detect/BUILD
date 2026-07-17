load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "detect",
    srcs = [
        "detect.go",
        "nondeterminism.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/detect",
    deps = [
        "//cli/arg",
        "//cli/explain",
        "//cli/log",
        "//cli/login",
        "//cli/parser/bazelrc",
        "//proto:buildbuddy_service_go_proto",
        "//proto:notification_go_proto",
        "//proto:spawn_diff_go_proto",
        "//server/util/grpc_client",
        "//server/util/shlex",
        "//server/util/status",
        "//server/util/uuid",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "detect_test",
    srcs = ["nondeterminism_test.go"],
    embed = [":detect"],
    deps = [
        "//cli/arg",
        "//cli/parser",
        "//cli/parser/test_data",
        "//proto:spawn_diff_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
