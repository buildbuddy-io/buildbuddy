load("@io_bazel_rules_docker//container:container.bzl", "container_push")

container_push(
    name = "push_run_script",
    format = "Docker",
    image = "@run_script_image//image:dockerfile_image.tar",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-build/run-script",  # Note flame-build, not flame-public.
    tag = "$(version)",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
