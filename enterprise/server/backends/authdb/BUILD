load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "authdb",
    srcs = ["authdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb",
    deps = [
        "//proto:group_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/status",
    ],
)
