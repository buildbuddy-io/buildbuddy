load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "default_execution_image",
    dockerfile = ":Dockerfile",
    exec_properties = {
        "container-image": "docker://gcr.io/cloud-marketplace/google/rbe-ubuntu16-04@sha256:5464e3e83dc656fc6e4eae6a01f5c2645f1f7e95854b3802b85e86484132d90e",
    },
    visibility = ["//visibility:public"],
)
