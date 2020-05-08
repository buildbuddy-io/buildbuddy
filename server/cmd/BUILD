load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

# Use the go_binary rule to create an executable from our main file. Depend on
# the static_content we created above so they are included.
go_binary(
    name = "buildbuddy",
    args = ["--config_file=config/buildbuddy.local.yaml"],
    data = [
        "//:config_files",
        "//app:app_bundle.min",
        "//static",
    ],
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.

container_image(
    name = "base_image",
    base = "@go_image_base//image",
    symlinks = {
        "config.yaml": "app/server/cmd/buildbuddy.runfiles/buildbuddy/config/buildbuddy.release.yaml",
    },
    visibility = ["//visibility:public"],
)

go_image(
    name = "buildbuddy_image",
    base = ":base_image",
    binary = ":buildbuddy",
    visibility = ["//visibility:public"],
)

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cmd",
    visibility = ["//visibility:private"],
    deps = [
        "//server/config:go_default_library",
        "//server/janitor:go_default_library",
        "//server/libmain:go_default_library",
        "//server/util/healthcheck:go_default_library",
    ],
)
