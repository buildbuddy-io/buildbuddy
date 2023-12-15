load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "run_script",
    dockerfile = ":Dockerfile",
    exec_properties = {
        "container-image": "docker://python:latest",
    },
)

container_push(
    name = "push_run_script",
    format = "Docker",
    image = "@run_script_image//image:dockerfile_image.tar",
    registry = "gcr.io",
    repository = "flame-build/run-script",  # Note flame-build, not flame-public.
    tag_file = "//deployment:image_tag",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
