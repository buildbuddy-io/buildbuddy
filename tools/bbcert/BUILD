load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "bbcert_lib",
    srcs = ["bbcert.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbcert",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:certgenerator_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "//tools/bbcert/update",
    ],
)

go_binary(
    name = "bbcert",
    embed = [":bbcert_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
    x_defs = {
        "github.com/buildbuddy-io/buildbuddy/tools/bbcert.defaultServers": "{STABLE_BBCERT_DEFAULT_SERVERS}",
        "github.com/buildbuddy-io/buildbuddy/tools/bbcert/update.baseURL": "{STABLE_BBCERT_UPDATE_URL}",
        # The embedded SHA is used to determine if an update is available.
        "github.com/buildbuddy-io/buildbuddy/tools/bbcert/update.commitSHA": "{STABLE_COMMIT_SHA}",
    },
)

go_test(
    name = "bbcert_test",
    srcs = ["bbcert_test.go"],
    embed = [":bbcert_lib"],
    deps = ["@com_github_stretchr_testify//require"],
)
