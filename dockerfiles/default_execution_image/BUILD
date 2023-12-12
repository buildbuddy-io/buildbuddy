load("//rules/kaniko:def.bzl", "remote_kaniko_build")

remote_kaniko_build(
    name = "default_execution_image",
    dockerfile = ":Dockerfile",
    visibility = ["//visibility:public"],
)
