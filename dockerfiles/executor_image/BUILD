load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "executor_image",
    dockerfile = ":Dockerfile",
    exec_properties = {
        "container-image": "docker://gcr.io/cloud-marketplace/google/debian11@sha256:69e2789c9f3d28c6a0f13b25062c240ee7772be1f5e6d41bb4680b63eae6b304",
    },
)
