load("@container_structure_test//:defs.bzl", "container_structure_test")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_filegroup", "pkg_files", "pkg_mklink")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

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

pkg_files(
    name = "binary_tar",
    srcs = [":buildbuddy"],
    attributes = pkg_attributes(mode = "0755"),
    include_runfiles = True,
    renames = {
        # Since the binary is located in /buildbuddy, it will collide with our
        # tmp_dir_symlink. Rename it to /server.
        # TODO(sluongng): Remove usage of rules_docker and change the go_binary name to "server"
        "buildbuddy": "server",
    },
)

pkg_mklink(
    name = "config_mklink",
    link_name = "config.yaml",
    target = "buildbuddy.runfiles/buildbuddy/config/buildbuddy.release.yaml",
)

pkg_mklink(
    name = "tmp_dir_mklink",
    link_name = "buildbuddy",
    target = "tmp",
)

pkg_tar(
    name = "tar",
    srcs = [
        ":binary_tar",
        ":config_mklink",
        ":tmp_dir_mklink",
    ],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/server"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)

container_structure_test(
    name = "oci_test",
    configs = [":oci_test.yaml"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
    },
    image = ":oci_image",
    tags = ["docker"],
)
