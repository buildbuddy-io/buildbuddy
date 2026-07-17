load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "density_lib",
    srcs = ["density.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/dns/cmd/density",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/dns/server",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/experiments",
        "//enterprise/server/experiments/gcsflagsync",
        "//server/backends/blobstore/gcs",
        "//server/config",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/status",
        "//server/util/tracing",
        "//server/version",
        "@com_github_miekg_dns//:dns",
    ],
)

go_binary(
    name = "density",
    embed = [":density_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "density_go_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":density",
    tags = ["manual"],
)

container_image(
    name = "density_image",
    base = ":density_go_image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
