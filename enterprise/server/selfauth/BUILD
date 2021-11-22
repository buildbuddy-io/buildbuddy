load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "selfauth",
    srcs = ["selfauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//server/config",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
        "@com_github_lestrrat_go_jwx//jwa",
        "@com_github_lestrrat_go_jwx//jwk",
        "@com_github_lestrrat_go_jwx//jwt",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
