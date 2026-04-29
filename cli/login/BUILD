load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "login",
    srcs = ["login.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/login",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/parser",
        "//cli/storage",
        "//cli/terminal",
        "//proto:buildbuddy_service_go_proto",
        "//proto:user_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "login_test",
    srcs = ["login_test.go"],
    embed = [":login"],
    deps = [
        "//cli/storage",
        "//server/testutil/testgit",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
