load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "clickhouseload_lib",
    srcs = ["clickhouseload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouseload",
    visibility = ["//visibility:private"],
    deps = [
        "//server/environment",
        "//server/real_environment",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/qps",
        "//server/util/uuid",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
    ],
)

go_binary(
    name = "clickhouseload",
    embed = [":clickhouseload_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "clickhouseload_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":clickhouseload",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
