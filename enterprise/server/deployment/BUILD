load("@io_bazel_rules_docker//container:push.bzl", "container_push")

package(default_visibility = ["//enterprise:__subpackages__"])

container_push(
    name = "push_ci_runner",
    format = "Docker",
    image = "//enterprise/server/cmd/ci_runner:ci_runner_image",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-ci-runner",
    tag = "$(version)",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

container_push(
    name = "push_ci_runner_debug",
    format = "Docker",
    image = "//enterprise/server/cmd/ci_runner:ci_runner_debug_image",
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-ci-runner",
    tag = "debug",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
