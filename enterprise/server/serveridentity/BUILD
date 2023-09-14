load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "serveridentity",
    srcs = ["serveridentity.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/serveridentity",
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "serveridentity_test",
    srcs = ["serveridentity_test.go"],
    deps = [
        ":serveridentity",
        "//server/interfaces",
        "//server/util/testing/flags",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
