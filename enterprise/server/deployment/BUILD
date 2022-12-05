load("@io_bazel_rules_docker//container:push.bzl", "container_push")

package(default_visibility = ["//enterprise:__subpackages__"])

container_push(
    name = "push_ci_runner",
    format = "Docker",
    image = "//enterprise/server/cmd/ci_runner:ci_runner_image",
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-ci-runner",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
