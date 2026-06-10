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
        "//cli/bazelisk",
        "//cli/log",
        "//cli/login",
        "//cli/parser/bazelrc",
        "//proto:spawn_diff_go_proto",
        "//server/util/shlex",
        "@org_golang_google_protobuf//proto",
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
        "@org_golang_google_protobuf//proto",
    ],
)
