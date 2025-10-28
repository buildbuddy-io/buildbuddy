load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vmdns",
    srcs = ["vmdns.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmdns",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/firecrackerutil",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "@com_github_miekg_dns//:dns",
    ],
)

go_test(
    name = "vmdns_test",
    srcs = ["vmdns_test.go"],
    deps = [
        ":vmdns",
        "//server/util/networking",
        "@com_github_miekg_dns//:dns",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
