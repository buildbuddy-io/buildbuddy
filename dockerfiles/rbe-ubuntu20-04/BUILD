load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "rbe-ubuntu20-04",
    dockerfile = ":Dockerfile",
    exec_properties = {
        "container-image": "docker://marketplace.gcr.io/google/ubuntu2004@sha256:9e3ead2510253310bd5b44d254ab810c8f37b19a35716379220fa72b38e8c818",
    },
)
