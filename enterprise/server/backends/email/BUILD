load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "email",
    srcs = ["email.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email",
    deps = [
        "//server/http/httpclient",
        "//server/util/flag",
        "//server/util/status",
        "@com_github_sendgrid_rest//:rest",
        "@com_github_sendgrid_sendgrid_go//:sendgrid-go",
        "@com_github_sendgrid_sendgrid_go//helpers/mail",
    ],
)

go_test(
    name = "email_test",
    size = "small",
    srcs = ["email_test.go"],
    deps = [
        ":email",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
