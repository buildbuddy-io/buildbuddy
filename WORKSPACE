workspace(
    name = "buildbuddy",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

# Bazel platforms

http_archive(
    name = "platforms",
    sha256 = "5308fc1d8865406a49427ba24a9ab53087f17f5266a7aabbfc28823f3916e1ca",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
    ],
)

# Go

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "ae013bf35bd23234d1dea46b079f1e05ba74ac0321423830119d3e787ec73483",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.36.0/rules_go-v0.36.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.36.0/rules_go-v0.36.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "448e37e0dbf61d6fa8f00aaa12d191745e14f07c31cabfa731f0c8e8a4f41b97",
    # Keep version in sync with .github/workflows/checkstyle.yaml
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.28.0/bazel-gazelle-v0.28.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.28.0.tar.gz",
    ],
)

load(":deps.bzl", "install_buildbuddy_dependencies")

# Install gazelle and go_rules dependencies after ours so that our go module versions take precedence.

# gazelle:repository_macro deps.bzl%install_buildbuddy_dependencies
install_buildbuddy_dependencies()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_download_sdk(
    name = "go_sdk_linux",
    goarch = "amd64",
    goos = "linux",
    version = "1.19.3",  # Keep in sync with .github/workflows/checkstyle.yaml
)

go_download_sdk(
    name = "go_sdk_linux_arm64",
    goarch = "arm64",
    goos = "linux",
    version = "1.19.3",
)

go_download_sdk(
    name = "go_sdk_darwin",
    goarch = "amd64",
    goos = "darwin",
    version = "1.19.3",
)

go_download_sdk(
    name = "go_sdk_darwin_arm64",
    goarch = "arm64",
    goos = "darwin",
    version = "1.19.3",
)

go_register_toolchains(
    nogo = "@//:vet",
)

gazelle_dependencies()

# Node

http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "2b2004784358655f334925e7eadc7ba80f701144363df949b3293e1ae7a2fb7b",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/5.4.0/rules_nodejs-5.4.0.tar.gz"],
)

load("@build_bazel_rules_nodejs//:repositories.bzl", "build_bazel_rules_nodejs_dependencies")

build_bazel_rules_nodejs_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = "16.9.0",
)

load("@rules_nodejs//nodejs:yarn_repositories.bzl", "yarn_repositories")

yarn_repositories(
    name = "yarn",
    yarn_version = "1.22.10",
)

load("@build_bazel_rules_nodejs//:index.bzl", "yarn_install")

yarn_install(
    name = "npm",
    exports_directories_only = False,
    package_json = "//:package.json",
    symlink_node_modules = False,
    yarn_lock = "//:yarn.lock",
)

# Docker

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "27d53c1d646fc9537a70427ad7b034734d08a9c38924cc6357cc973fed300820",
    strip_prefix = "rules_docker-0.24.0",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.24.0/rules_docker-v0.24.0.tar.gz"],
)

load(
    "@io_bazel_rules_docker//toolchains/docker:toolchain.bzl",
    docker_toolchain_configure = "toolchain_configure",
)

docker_toolchain_configure(
    name = "docker_config",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load(
    "@io_bazel_rules_docker//go:image.bzl",
    _go_image_repos = "repositories",
)

_go_image_repos()

# rules_foreign_cc

http_archive(
    name = "rules_foreign_cc",
    sha256 = "e14a159c452a68a97a7c59fa458033cc91edb8224516295b047a95555140af5f",
    strip_prefix = "rules_foreign_cc-0.4.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/0.4.0.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

# This sets up some common toolchains for building targets. For more details, please see
# https://bazelbuild.github.io/rules_foreign_cc/0.4.0/flatten.html#rules_foreign_cc_dependencies
rules_foreign_cc_dependencies()

all_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

# CPIO

http_archive(
    name = "org_gnu_cpio",
    build_file_content = all_content,
    sha256 = "e87470d9c984317f658567c03bfefb6b0c829ff17dbf6b0de48d71a4c8f3db88",
    strip_prefix = "cpio-2.13",
    url = "https://ftp.gnu.org/gnu/cpio/cpio-2.13.tar.gz",
)

# Kubernetes

http_archive(
    name = "io_bazel_rules_k8s",
    sha256 = "ce5b9bc0926681e2e7f2147b49096f143e6cbc783e71bc1d4f36ca76b00e6f4a",
    strip_prefix = "rules_k8s-0.7",
    urls = ["https://github.com/bazelbuild/rules_k8s/archive/refs/tags/v0.7.tar.gz"],
)

load("@io_bazel_rules_k8s//k8s:k8s.bzl", "k8s_defaults", "k8s_repositories")

k8s_repositories()

load("@io_bazel_rules_k8s//k8s:k8s_go_deps.bzl", k8s_go_deps = "deps")

k8s_go_deps(go_version = "")

k8s_defaults(
    name = "k8s_deploy",
    kind = "deployment",
)

# Proto

# NB: The name must be "com_google_protobuf".
http_archive(
    name = "com_google_protobuf",
    sha256 = "7c9731ff49ebe1cc4a0650a21d40acc099043f4d584b24632bafc0f5328bc3ff",
    strip_prefix = "protobuf-3.19.0",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.19.0/protobuf-all-3.19.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@io_bazel_rules_docker//contrib:dockerfile_build.bzl", "dockerfile_image")

dockerfile_image(
    name = "default_execution_image",
    dockerfile = "//dockerfiles/default_execution_image:Dockerfile",
    visibility = ["//visibility:public"],
)

dockerfile_image(
    name = "executor_image",
    dockerfile = "//dockerfiles/executor_image:Dockerfile",
    visibility = ["//visibility:public"],
)

dockerfile_image(
    name = "nonroot_user_image",
    dockerfile = "//dockerfiles/test_images/nonroot_user_image:Dockerfile",
    visibility = ["//visibility:public"],
)

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

container_pull(
    name = "buildbuddy_go_image_base",
    digest = "sha256:34e96e21516698913035a62ef1ce484d91184de8a44209a33c7e134547e20dee",
    registry = "gcr.io",
    repository = "distroless/base-debian11",
)

# Base image that can be used to build images that are capable of running the Bazel binary.
container_pull(
    name = "bazel_image_base",
    digest = "sha256:ab0c5fbe16bc01c03eb081a5724ba618110cbd24940ab123a8dbee0382a4c175",
    registry = "gcr.io",
    repository = "distroless/java11-debian11",
)

dockerfile_image(
    name = "ci_runner_image",
    dockerfile = "//enterprise/dockerfiles/ci_runner_image:Dockerfile",
    visibility = ["//visibility:public"],
)

dockerfile_image(
    name = "run_script_image",
    dockerfile = "//dockerfiles/run_script:Dockerfile",
    visibility = ["//visibility:public"],
)

# BuildBuddy Toolchain

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "656985ebceaa0116adcae081b19becf41f246893759ac41f86267cb2220f966e",
    strip_prefix = "buildbuddy-toolchain-2a9769e75878519bf48c12334214501d4859154b",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/2a9769e75878519bf48c12334214501d4859154b.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")

http_archive(
    name = "cloudprober",
    build_file_content = "exports_files([\"cloudprober\"])",
    sha256 = "0a824a6e224d9810514f4a2f4a13f09488672ad483bb0e978c16d8a6b3372625",
    strip_prefix = "cloudprober-v0.11.2-ubuntu-x86_64",
    urls = ["https://github.com/google/cloudprober/releases/download/v0.11.2/cloudprober-v0.11.2-ubuntu-x86_64.zip"],
)

# esbuild (for bundling JS)

load("@build_bazel_rules_nodejs//toolchains/esbuild:esbuild_repositories.bzl", "esbuild_repositories")

esbuild_repositories(npm_repository = "npm")

# SWC (for transpiling TS -> JS)

http_archive(
    name = "aspect_rules_swc",
    sha256 = "206a89aae3a04831123b43962a3864e8ab1652b703c4af58d84b04174360137d",
    strip_prefix = "rules_swc-0.4.0",
    url = "https://github.com/aspect-build/rules_swc/archive/refs/tags/v0.4.0.tar.gz",
)

load("@aspect_rules_swc//swc:dependencies.bzl", "rules_swc_dependencies")

rules_swc_dependencies()

load("@aspect_rules_swc//swc:repositories.bzl", "swc_register_toolchains")

swc_register_toolchains(
    name = "swc",
    swc_version = "v1.2.141",
)

# Web testing

http_archive(
    name = "io_bazel_rules_webtesting",
    sha256 = "e9abb7658b6a129740c0b3ef6f5a2370864e102a5ba5ffca2cea565829ed825a",
    urls = [
        "https://github.com/bazelbuild/rules_webtesting/releases/download/0.3.5/rules_webtesting.tar.gz",
    ],
)

load("@io_bazel_rules_webtesting//web:repositories.bzl", "web_test_repositories")

web_test_repositories()

load("@io_bazel_rules_webtesting//web/versioned:browsers-0.3.3.bzl", "browser_repositories")

browser_repositories(chromium = True)

load("@io_bazel_rules_webtesting//web:go_repositories.bzl", web_test_go_repositories = "go_repositories")

web_test_go_repositories()

# AWS RDS instance certs are signed by an AWS CA.
# The cert is necessary to validate connections to AWS RDS instances when TLS is enabled.
http_file(
    name = "aws_rds_certs",
    downloaded_file_path = "rds-combined-ca-bundle.pem",
    sha256 = "6a8ba1c9f858386edba0ea82b7bf8168ef513d1eb0df3a08cc7cf4bb89f856d0",
    url = "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem",
)
