load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "fix",
    srcs = [
        "fix.go",
        "test.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    # keep
    deps = [
        "//cli/add",
        "//cli/arg",
        "//cli/ask",
        "//cli/bazelisk",
        "//cli/detect",
        "//cli/fix/langs:gazelle",
        "//cli/fix/language",
        "//cli/log",
        "//cli/login",
        "//cli/remotebazel",
        "//cli/terminal",
        "//cli/translate",
        "//cli/util/agent",
        "//cli/util/agent/agentutil",
        "//cli/util/download",
        "//cli/view",
        "//cli/workspace",
        "//proto:buildbuddy_service_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "@com_github_bazelbuild_buildtools//buildifier:buildifier_lib",
        "@com_github_bazelbuild_buildtools//edit",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)
