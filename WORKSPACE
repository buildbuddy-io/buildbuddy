workspace(
    name = "buildbuddy",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

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
    sha256 = "884fa7b014167fed6bf7cb8520652e364a904e57d3ce172419af30db5ce6217c",
    strip_prefix = "rules_go-5206498b4f67ff3e6a9222a923cf67ff5191754e",
    urls = [
        # TODO(sluongng): this track the unreleased version v0.42.0 of rules_go to help us upgrade to Go 1.21
        # We should replace this once rules_go v0.42.0 is released.
        "https://github.com/bazelbuild/rules_go/archive/5206498b4f67ff3e6a9222a923cf67ff5191754e.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:gazelle.patch",
    ],
    sha256 = "29218f8e0cebe583643cbf93cae6f971be8a2484cdcfa1e45057658df8d54002",
    # Keep version in sync with .github/workflows/checkstyle.yaml
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.32.0/bazel-gazelle-v0.32.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.32.0/bazel-gazelle-v0.32.0.tar.gz",
    ],
)

load(":deps.bzl", "install_go_mod_dependencies", "install_static_dependencies")

install_static_dependencies()

# Install gazelle and go_rules dependencies after ours so that our go module versions take precedence.

# gazelle:repository_macro deps.bzl%install_go_mod_dependencies
install_go_mod_dependencies()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

# Register multiple Go SDKs so that we can perform cross-compilation remotely.
# i.e. We might want to trigger a Linux AMD64 Go build remotely from a MacOS ARM64 laptop.
#
# Reference: https://github.com/bazelbuild/rules_go/issues/3540.
go_download_sdk(
    name = "go_sdk_linux",
    goarch = "amd64",
    goos = "linux",
    version = "1.21.1",  # Keep in sync with .github/workflows/checkstyle.yaml
)

go_download_sdk(
    name = "go_sdk_linux_arm64",
    goarch = "arm64",
    goos = "linux",
    version = "1.21.1",
)

go_download_sdk(
    name = "go_sdk_darwin",
    goarch = "amd64",
    goos = "darwin",
    version = "1.21.1",
)

go_download_sdk(
    name = "go_sdk_darwin_arm64",
    goarch = "arm64",
    goos = "darwin",
    version = "1.21.1",
)

go_download_sdk(
    name = "go_sdk_windows",
    goarch = "amd64",
    goos = "windows",
    version = "1.21.1",
)

go_download_sdk(
    name = "go_sdk_windows_arm64",
    goarch = "arm64",
    goos = "windows",
    version = "1.21.1",
)

go_register_toolchains(
    nogo = "@//:vet",
)

gazelle_dependencies()

http_archive(
    name = "googleapis",
    # TODO(sluongng): remove this once https://github.com/googleapis/googleapis/pull/831 is merged.
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:googleapis-bytestream-proto.patch",
    ],
    sha256 = "9d1a930e767c93c825398b8f8692eca3fe353b9aaadedfbcf1fca2282c85df88",
    strip_prefix = "googleapis-64926d52febbf298cb82a8f472ade4a3969ba922",
    urls = [
        "https://github.com/googleapis/googleapis/archive/64926d52febbf298cb82a8f472ade4a3969ba922.zip",
    ],
)

load("@googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
)

# Golang staticcheck

http_archive(
    name = "com_github_sluongng_nogo_analyzer",
    sha256 = "a74a5e44751d292d17bd879e5aa8b40baa94b5dc2f043df1e3acbb3e23ead073",
    strip_prefix = "nogo-analyzer-0.0.2",
    urls = [
        "https://github.com/sluongng/nogo-analyzer/archive/refs/tags/v0.0.2.tar.gz",
    ],
)

load("@com_github_sluongng_nogo_analyzer//staticcheck:deps.bzl", "staticcheck")

staticcheck()

# Node

http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "94070eff79305be05b7699207fbac5d2608054dd53e6109f7d00d923919ff45a",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.2/rules_nodejs-5.8.2.tar.gz"],
)

load("@build_bazel_rules_nodejs//:repositories.bzl", "build_bazel_rules_nodejs_dependencies")

build_bazel_rules_nodejs_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = "16.19.0",
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

# Proto -- must be before container_repositories so we don't inherit their rules_pkg.

# NB: The name must be "com_google_protobuf".

# Required by com_google_protobuf
http_archive(
    name = "com_google_googletest",
    sha256 = "730215d76eace9dd49bf74ce044e8daa065d175f1ac891cc1d6bb184ef94e565",
    strip_prefix = "googletest-f53219cdcb7b084ef57414efea92ee5b71989558",
    urls = [
        "https://github.com/google/googletest/archive/f53219cdcb7b084ef57414efea92ee5b71989558.tar.gz",
    ],
)

load("@com_google_googletest//:googletest_deps.bzl", "googletest_deps")

googletest_deps()

http_archive(
    name = "com_google_protobuf",
    sha256 = "b29fc5fc13926f347b7a8b676ae1e63f7ccdb92c2fc8ca326bc3a883dcc168ac",
    strip_prefix = "protobuf-23.0",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v23.0/protobuf-23.0.tar.gz"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Docker

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
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
    digest = "sha256:3172df37ef8caa768ce74ebbc7f0e2b6a2641d3b35d18659d36f3815e30fe620",
    registry = "gcr.io",
    repository = "distroless/cc-debian11",
)

# Base image that can be used to build images that are capable of running the Bazel binary.
container_pull(
    name = "bazel_image_base",
    digest = "sha256:ab0c5fbe16bc01c03eb081a5724ba618110cbd24940ab123a8dbee0382a4c175",
    registry = "gcr.io",
    repository = "distroless/java11-debian11",
)

dockerfile_image(
    name = "rbe-ubuntu20-04_image",
    dockerfile = "//dockerfiles/rbe-ubuntu20-04:Dockerfile",
)

dockerfile_image(
    name = "rbe-ubuntu20-04-webtest_image",
    dockerfile = "//dockerfiles/rbe-ubuntu20-04-webtest:Dockerfile",
)

dockerfile_image(
    name = "ci_runner_image",
    dockerfile = "//enterprise/dockerfiles/ci_runner_image:Dockerfile",
    visibility = ["//visibility:public"],
)

dockerfile_image(
    name = "rbe-ubuntu20-04-workflows_image",
    dockerfile = "//enterprise/dockerfiles/rbe-ubuntu20-04-workflows:Dockerfile",
    visibility = ["//visibility:public"],
)

dockerfile_image(
    name = "run_script_image",
    dockerfile = "//dockerfiles/run_script:Dockerfile",
    visibility = ["//visibility:public"],
)

# BuildBuddy Toolchain
# Keep up-to-date with docs/rbe-setup.md and docs/rbe-github-actions.md

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "1cab6ef3ae9b4211ab9d57826edd4bbc34e5b9e5cb1927c97f0788d8e7ad0442",
    strip_prefix = "buildbuddy-toolchain-b043878a82f266fd78369b794a105b57dc0b2600",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/b043878a82f266fd78369b794a105b57dc0b2600.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    container_image = UBUNTU20_04_IMAGE,
)

http_archive(
    name = "cloudprober",
    build_file_content = "exports_files([\"cloudprober\"])",
    sha256 = "0a824a6e224d9810514f4a2f4a13f09488672ad483bb0e978c16d8a6b3372625",
    strip_prefix = "cloudprober-v0.11.2-ubuntu-x86_64",
    urls = ["https://github.com/google/cloudprober/releases/download/v0.11.2/cloudprober-v0.11.2-ubuntu-x86_64.zip"],
)

# protoc-gen-protobufjs (for .proto to .js codegen)
http_archive(
    name = "com_github_buildbuddy_io_protoc_gen_protobufjs",
    sha256 = "ef3f8fb9a417fa7c1e2fe53003221f118214cf014294bfbc5dcfbb2ed1560e83",
    urls = ["https://github.com/buildbuddy-io/protoc-gen-protobufjs/releases/download/v0.0.9/protoc-gen-protobufjs-v0.0.9.tar.gz"],
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

# Proto rules

http_archive(
    name = "rules_proto",
    sha256 = "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
    strip_prefix = "rules_proto-5.3.0-21.7",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()
