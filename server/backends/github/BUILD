load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/github",
    visibility = ["//visibility:public"],
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/cookie",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_google_go_github_v59//github",
    ],
)
