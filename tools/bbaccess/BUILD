load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "bbaccess_lib",
    srcs = ["bbaccess.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:certgenerator_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "//tools/bbaccess/update",
    ],
)

go_binary(
    name = "bbaccess",
    embed = [":bbaccess_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
    x_defs = {
        # The embedded SHA is used to determine if an update is available.
        "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/update.commitSHA": "{STABLE_COMMIT_SHA}",
    },
)

go_test(
    name = "bbaccess_test",
    srcs = ["bbaccess_test.go"],
    embed = [":bbaccess_lib"],
    deps = ["@com_github_stretchr_testify//require"],
)
