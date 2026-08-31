load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "processlist",
    srcs = ["processlist_openbsd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/processlist",
    target_compatible_with = ["@platforms//os:openbsd"],
    visibility = [
        "//enterprise/server/remote_execution/commandutil:__pkg__",
        "//enterprise/server/util/procstats:__pkg__",
    ],
    deps = ["@org_golang_x_sys//unix"],
)

go_test(
    name = "processlist_test",
    srcs = ["processlist_openbsd_test.go"],
    embed = [":processlist"],
    target_compatible_with = ["@platforms//os:openbsd"],
)
