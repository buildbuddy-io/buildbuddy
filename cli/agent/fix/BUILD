load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent/fix",
    deps = [
        "//cli/agent/agentflags",
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/parser",
        "//cli/parser/arguments",
        "//cli/parser/parsed",
        "//cli/terminal",
        "//cli/util/agent",
        "//cli/util/agent/agentutil",
        "//cli/util/download",
        "//cli/util/invocation",
        "//cli/view",
        "//cli/watcher/option_definitions",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/uuid",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "fix_test",
    srcs = ["fix_test.go"],
    embed = [":fix"],
    deps = [
        "//cli/parser",
        "//cli/parser/test_data",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
