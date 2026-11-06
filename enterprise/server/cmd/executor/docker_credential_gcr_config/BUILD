load("@rules_shell//shell:sh_test.bzl", "sh_test")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "config.json",
    "docker-credential-gcr-optional",
])

filegroup(
    name = "docker_credential_gcr",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["@com_github_googlecloudplatform_docker-credential-gcr-linux-amd64//:docker-credential-gcr.bin"],
        "//platforms/configs:linux_arm64": ["@com_github_googlecloudplatform_docker-credential-gcr-linux-arm64//:docker-credential-gcr.bin"],
        "//conditions:default": [],
    }),
    target_compatible_with = select({
        "//platforms/configs:linux_x86_64": [],
        "//platforms/configs:linux_arm64": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)

sh_test(
    name = "docker_credential_gcr_optional_test",
    size = "small",
    srcs = ["docker_credential_gcr_optional_test.sh"],
    args = [
        "$(rlocationpath :docker-credential-gcr-optional)",
        "$(rlocationpath :docker_credential_gcr)",
    ],
    data = [
        ":docker-credential-gcr-optional",
        ":docker_credential_gcr",
    ],
    exec_compatible_with = ["@platforms//os:linux"],
    tags = ["block-network"],
    deps = ["@bazel_tools//tools/bash/runfiles"],
)
