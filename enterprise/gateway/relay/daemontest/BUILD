load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "daemontest_test",
    srcs = ["daemon_e2e_test.go"],
    deps = [
        "//enterprise/gateway/certauth",
        "//enterprise/gateway/relay",
        "//enterprise/gateway/server",
        "//proto:gateway_service_go_proto",
        "//server/testutil/testauthrelay",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "//tools/bbaccess/tunnel/credentials",
        "//tools/bbaccess/tunnel/dnsserver",
        "//tools/bbaccess/tunnel/fakeip",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "//tools/bbaccess/tunnel/tunnelmgr",
        "@com_github_miekg_dns//:dns",
        "@com_github_stretchr_testify//require",
    ],
)
