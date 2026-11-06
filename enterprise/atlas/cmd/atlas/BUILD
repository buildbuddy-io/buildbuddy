load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "atlas_lib",
    srcs = ["atlas.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/cmd/atlas",
    visibility = ["@buildbuddy_internal//:__subpackages__"],
    deps = [
        "//enterprise/atlas/app:bundle",
        "//enterprise/atlas/server/atlas_service",
        "//enterprise/atlas/server/cluster",
        "//enterprise/atlas/server/summaries",
        "//enterprise/atlas/server/web",
        "//enterprise/server/backends/configsecrets",
        "//proto:atlas_go_proto",
        "//server/config",
        "//server/interfaces",
        "//server/nullauth",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
    ],
)

# In fastbuild the app is served from these runfiles; release builds embed
# them into the binary instead (see //enterprise/atlas/app:bundle).
filegroup(
    name = "runfiles",
    data = [
        "//enterprise/atlas/app:app_bundle",
        "//enterprise/atlas/app:sha",
        "//enterprise/atlas/app:style",
    ],
)

go_binary(
    name = "atlas",
    data = [":runfiles"],
    embed = [":atlas_lib"],
    visibility = ["//visibility:public"],
)
