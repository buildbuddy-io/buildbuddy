load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")

# The bash image installs bash in /usr/local/bin, so link /bin/bash for
# run_script.sh's shebang.
container_image(
    name = "image",
    base = "@bash_image//image",
    cmd = ["/run_script.sh"],
    entrypoint = [],
    files = ["run_script.sh"],
    symlinks = {"/bin/bash": "/usr/local/bin/bash"},
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

container_push(
    name = "push_run_script",
    format = "Docker",
    image = ":image",
    registry = "gcr.io",
    repository = "flame-build/run-script",  # Note flame-build, not flame-public.
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
