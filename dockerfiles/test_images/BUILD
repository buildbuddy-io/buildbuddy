load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//container:push.bzl", "container_push")

# Rules to build and push test images

# Definitions for a docker image with a non-root user as the default user.

container_image(
    name = "nonroot_user_container_image",
    base = "@nonroot_user_image//image:dockerfile_image.tar",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

container_push(
    name = "nonroot_user_push",
    format = "Docker",
    image = "//dockerfiles/test_images:nonroot_user_container_image",
    registry = "gcr.io",
    repository = "flame-build/test-nonroot-user",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
