load("@io_bazel_rules_docker//container:push.bzl", "container_push")

# Release

# Definition for uploading new releases of the on_prem docker image.

container_push(
    name = "release_enterprise",
    format = "Docker",
    image = "//enterprise/server/cmd/server:buildbuddy_image",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-app-enterprise",
    tag = "$(version)",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

# Definition for uploading new releases of the on_prem executor docker image.

container_push(
    name = "release_executor_enterprise",
    format = "Docker",
    image = "//enterprise/server/cmd/executor:executor_image",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-executor-enterprise",
    tag = "$(version)",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

# Definitions for uploading new default base docker image.
container_push(
    name = "executor_docker_default",
    format = "Docker",
    image = "//enterprise/server/cmd/executor:default_base_image",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-public/executor-docker-default",  # Note flame-build, not flame-public.
    tag = "$(version)",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

container_push(
    name = "rbe-ubuntu18-04",
    format = "Docker",
    image = "@rbe-ubuntu18-04_image//image:dockerfile_image",
    registry = "gcr.io",
    repository = "flame-public/rbe-ubuntu18-04",
    tag = "$(version)",
    tags = ["manual"],
)
