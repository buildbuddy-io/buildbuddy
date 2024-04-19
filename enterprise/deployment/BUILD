load("@io_bazel_rules_docker//container:push.bzl", "container_push")

# Release

# Definitions for uploading new default base docker image.
container_push(
    name = "executor_docker_default",
    format = "Docker",
    image = "//enterprise/server/cmd/executor:default_base_image",
    registry = "gcr.io",
    repository = "flame-public/executor-docker-default",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

container_push(
    name = "rbe-ubuntu20-04",
    format = "Docker",
    image = "@rbe-ubuntu20-04_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-public/rbe-ubuntu20-04",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],
)

container_push(
    name = "rbe-ubuntu20-04-webtest",
    format = "Docker",
    image = "@rbe-ubuntu20-04-webtest_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-public/rbe-ubuntu20-04-webtest",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
