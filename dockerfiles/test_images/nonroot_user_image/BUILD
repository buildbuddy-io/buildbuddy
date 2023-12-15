load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "nonroot_user_image",
    dockerfile = ":Dockerfile",
    exec_properties = {
        "container-image": "docker://busybox",
    },
)
