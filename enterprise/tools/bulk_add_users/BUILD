load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bulk_add_users_lib",
    srcs = ["bulk_add_users.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/bulk_add_users",
    visibility = ["//visibility:private"],
    deps = [
        "//server/real_environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/log",
    ],
)

go_binary(
    name = "bulk_add_users",
    embed = [":bulk_add_users_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
