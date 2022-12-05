load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")

container_image(
    name = "probers_image",
    base = "@bazel_image_base//image",
    entrypoint = [
        "/cloudprober",
        "--logtostderr",
    ],
    files = [
        "//tools/probers/bazelrbe",
        "@cloudprober",
    ],
    tars = [
        "//server/util/bazel:bazel_binaries_tar",
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
