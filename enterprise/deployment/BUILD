load("@io_bazel_rules_docker//container:push.bzl", "container_push")

package(default_visibility = ["//enterprise:__subpackages__"])

# IMPORTANT: When pushing these images, make sure your local docker version is
# <= v24, otherwise the images will use a newer oci mediaType for the pushed
# layers which we don't yet properly handle in some cases (e.g. ext4 image
# conversion for firecracker).

# Default action execution image (based on ubuntu 16.04).
# This is not to be confused with the *executor* image, which is based on debian.
container_push(
    name = "executor_docker_default",
    format = "Docker",
    image = "//enterprise/server/cmd/executor:default_base_image",
    registry = "gcr.io",
    repository = "flame-public/executor-docker-default",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],
)

# Default workflows image (based on ubuntu 18.04).
container_push(
    name = "buildbuddy_ci_runner",
    format = "Docker",
    image = "//enterprise/server/cmd/ci_runner:ci_runner_image",
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-ci-runner",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],
)

# Ubuntu 20.04 action execution image.
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

# Ubuntu 20.04 workflows image.
# TODO: move 'buildbuddy' user provisioning into the rbe-ubuntu20-04 image,
# and remove this image - it's no longer needed since the ci_runner provisions
# bazelisk itself.
container_push(
    name = "rbe-ubuntu20-04-workflows",
    format = "Docker",
    image = "@rbe-ubuntu20-04-workflows_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-public/rbe-ubuntu20-04-workflows",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],
)

# Ubuntu 20.04 webtest image.
# This includes several packages needed by chromium.
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
