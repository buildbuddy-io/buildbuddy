load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

# Use the go_binary rule to create an executable from our main file. Depend on
# the static_content we created above so they are included.
go_binary(
    name = "buildbuddy",
    args = [
        "--config_file=config/buildbuddy.local.yaml",
        "--max_shutdown_duration=3s",
        "--static_directory=/static",
        "--app_directory=/app",
    ],
    data = [
        "//app:app_bundle",
        "//app:sha",
        "//app:style.css",
        "//config:config_files",
        "//static",
    ],
    embed = [":buildbuddy_lib"],
    visibility = ["//visibility:public"],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    symlinks = {
        "config.yaml": "app/server/cmd/buildbuddy/buildbuddy.runfiles/buildbuddy/config/buildbuddy.release.yaml",
        "buildbuddy": "tmp",
    },
    tags = [
        "manual",
        "no-cache",
    ],
    visibility = ["//visibility:public"],
)

# The go_image below can be used to build and run the server image.
# The container_image target just builds and tags the image without running it.

go_image(
    name = "buildbuddy_go_image",
    base = ":base_image",
    binary = ":buildbuddy",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

container_image(
    name = "buildbuddy_image",
    base = ":buildbuddy_go_image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "buildbuddy_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cmd/buildbuddy",
    visibility = [":__subpackages__"],
    deps = [
        "//app:bundle",
        "//server/config",
        "//server/janitor",
        "//server/libmain",
        "//server/telemetry",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/version",
    ],
)
