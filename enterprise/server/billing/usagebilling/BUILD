load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usagebilling",
    srcs = ["usagebilling.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/usagebilling",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/billing/metronome",
        "//enterprise/server/billing/stripe",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/status",
    ],
)
