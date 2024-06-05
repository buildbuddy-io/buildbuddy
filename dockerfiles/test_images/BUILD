load("@io_bazel_rules_docker//container:push.bzl", "container_push")

# Rules to build and push test images

container_push(
    name = "nonroot_user_push",
    format = "Docker",
    image = "@nonroot_user_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-build/test-nonroot-user",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

container_push(
    name = "xfsprogs_push",
    format = "Docker",
    image = "@xfsprogs_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-public/test-xfsprogs",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
