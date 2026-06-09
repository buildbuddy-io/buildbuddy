load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

container_image(
    name = "probers_image",
    base = "@bazel_image_base//image",
    entrypoint = [
        "/cloudprober",
        "--logtostderr",
    ],
    files = [
        "//tools/probers/bazelcache",
        "//tools/probers/bazelrbe",
        "//tools/probers/remote_runner",
        "@cloudprober",
    ],
    tags = ["manual"],
    tars = [
        "//server/util/bazel:prober_bazel_binaries_tar",
    ],
    visibility = ["//visibility:public"],
)

container_push(
    name = "push_probers_image",
    format = "Docker",
    image = ":probers_image",
    registry = "gcr.io",
    repository = "flame-build/probers",  # Note flame-build, not flame-public.
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

pkg_tar(
    name = "files_tar",
    srcs = [
        "//tools/probers/bazelcache",
        "//tools/probers/bazelrbe",
        "//tools/probers/remote_runner",
        "@cloudprober",
    ],
)

oci_image(
    name = "probers_oci_image",
    base = "@bazel_oci_image_base",
    entrypoint = [
        "/cloudprober",
        "--logtostderr",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    tars = [
        "//server/util/bazel:prober_bazel_binaries_tar",
        ":files_tar",
    ],
    visibility = ["//visibility:public"],
)

sh_test(
    name = "prober_image_inputs_test",
    srcs = ["prober_image_inputs_test.sh"],
    args = [
        "$(rootpath //server/util/bazel:prober_bazel_binaries_tar)",
        "$(rootpath :files_tar)",
    ],
    data = [
        ":files_tar",
        "//server/util/bazel:prober_bazel_binaries_tar",
    ],
)

oci_push(
    name = "push_probers_oci_image",
    image = ":probers_oci_image",
    remote_tags = "//deployment:oci_tag.txt",
    repository = "gcr.io/flame-build/probers-oci",
)
