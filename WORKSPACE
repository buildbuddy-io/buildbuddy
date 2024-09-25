workspace(
    name = "buildbuddy",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# Bazel platforms

http_archive(
    name = "platforms",
    sha256 = "218efe8ee736d26a3572663b374a253c012b716d8af0c07e842e82f238a0a7ee",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
    ],
)

http_archive(
    name = "bazel_features",
    sha256 = "cec7fbc7bce6597cf2e83e01ddd9328a1bb057dc1a3092745238f49d3301ab5a",
    strip_prefix = "bazel_features-1.12.0",
    url = "https://github.com/bazel-contrib/bazel_features/releases/download/v1.12.0/bazel_features-v1.12.0.tar.gz",
)

load("@bazel_features//:deps.bzl", "bazel_features_deps")

bazel_features_deps()

http_archive(
    name = "rules_pkg",
    integrity = "sha256-0gyVGWDtd8t7NBwqWUiFNOSU1a0dMMSBjHNtV3cqn+8=",
    url = "https://github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
)

# Proto rules

http_archive(
    name = "rules_proto",
    sha256 = "303e86e722a520f6f326a50b41cfc16b98fe6d1955ce46642a5b7a67c11c0f5d",
    strip_prefix = "rules_proto-6.0.0",
    url = "https://github.com/bazelbuild/rules_proto/releases/download/6.0.0/rules_proto-6.0.0.tar.gz",
)

# Go

# keep in sync with go.mod
http_archive(
    name = "io_bazel_rules_go",
    integrity = "sha256-9KkxRRjKas+hbMSrQ7C4zh5OpkuBw42KN3KIPxUzRrg=",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.50.1/rules_go-v0.50.1.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.50.1/rules_go-v0.50.1.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    integrity = "sha256-LHVFzJFKQR5cFcPVWgpe00+/9i3vDh8Ktu0UvaIiw8w=",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:gazelle.patch",
    ],
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.39.0/bazel-gazelle-v0.39.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.39.0/bazel-gazelle-v0.39.0.tar.gz",
    ],
)

http_archive(
    name = "com_google_absl",
    integrity = "sha256-9Q5awxGoE4Laf6dblzEOS5AGR0+VYKxG9UqZZ/B9SuM=",
    strip_prefix = "abseil-cpp-20240722.0",
    urls = ["https://github.com/abseil/abseil-cpp/releases/download/20240722.0/abseil-cpp-20240722.0.tar.gz"],
)

load(":deps.bzl", "install_go_mod_dependencies", "install_static_dependencies")

install_static_dependencies()

# Install gazelle and go_rules dependencies after ours so that our go module versions take precedence.

# gazelle:repository_macro deps.bzl%install_go_mod_dependencies
install_go_mod_dependencies()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

GO_SDK_VERSION = "1.23.1"

# Register multiple Go SDKs so that we can perform cross-compilation remotely.
# i.e. We might want to trigger a Linux AMD64 Go build remotely from a MacOS ARM64 laptop.
#
# Reference: https://github.com/bazelbuild/rules_go/issues/3540.
go_download_sdk(
    name = "go_sdk_linux",
    goarch = "amd64",
    goos = "linux",
    version = GO_SDK_VERSION,
)

go_download_sdk(
    name = "go_sdk_linux_arm64",
    goarch = "arm64",
    goos = "linux",
    version = GO_SDK_VERSION,
)

go_download_sdk(
    name = "go_sdk_darwin",
    goarch = "amd64",
    goos = "darwin",
    version = GO_SDK_VERSION,
)

go_download_sdk(
    name = "go_sdk_darwin_arm64",
    goarch = "arm64",
    goos = "darwin",
    version = GO_SDK_VERSION,
)

go_download_sdk(
    name = "go_sdk_windows",
    goarch = "amd64",
    goos = "windows",
    version = GO_SDK_VERSION,
)

go_download_sdk(
    name = "go_sdk_windows_arm64",
    goarch = "arm64",
    goos = "windows",
    version = GO_SDK_VERSION,
)

go_register_toolchains(
    nogo = "@//:vet",
)

gazelle_dependencies(
    go_env = {
        "GOPROXY": "https://proxy.golang.org|direct",
    },
)

# Version taken from
# https://github.com/bazelbuild/bazel-central-registry/blob/main/modules/googleapis/0.0.0-20240326-1c8d509c5/source.json
# So that we can use the same version of googleapis as the one used in bzlmod.
http_archive(
    name = "googleapis",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:googleapis.patch",
    ],
    sha256 = "b854ae17ddb933c249530f743db8d78df80905dfb42681255564a1d1921dfc3c",
    strip_prefix = "googleapis-1c8d509c574aeab7478be1bfd4f2e8f0931cfead",
    urls = [
        "https://github.com/googleapis/googleapis/archive/1c8d509c574aeab7478be1bfd4f2e8f0931cfead.tar.gz",
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
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:build_bazel_rules_nodejs.patch",
    ],
    sha256 = "94070eff79305be05b7699207fbac5d2608054dd53e6109f7d00d923919ff45a",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.2/rules_nodejs-5.8.2.tar.gz"],
)

http_archive(
    name = "rules_nodejs",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:build_bazel_rules_nodejs.patch",
    ],
    sha256 = "764a3b3757bb8c3c6a02ba3344731a3d71e558220adcb0cf7e43c9bba2c37ba8",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.2/rules_nodejs-core-5.8.2.tar.gz"],
)

load("@build_bazel_rules_nodejs//:repositories.bzl", "build_bazel_rules_nodejs_dependencies")

build_bazel_rules_nodejs_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = "18.13.0",
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
    integrity = "sha256-4BBolY8Sl0eYin++Cd3nzGSXTjs1Mx7kHuKCnwlh1HI=",
    patches = [
        # https://github.com/protocolbuffers/protobuf/pull/18241
        "//buildpatches:com_google_protobuf_18241.patch",
        # https://github.com/protocolbuffers/protobuf/pull/18242
        "//buildpatches:com_google_protobuf_18242.patch",
        # https://github.com/protocolbuffers/protobuf/pull/18243
        "//buildpatches:com_google_protobuf_18243.patch",
    ],
    strip_prefix = "protobuf-28.2",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v28.2/protobuf-28.2.zip"],
)

http_archive(
    name = "zlib",
    # patch_args = ["-p1"],
    patches = [
        # from https://github.com/bazelbuild/bazel-central-registry/blob/main/modules/zlib/1.3.1.bcr.3/patches/add_build_file.patch
        "//buildpatches:zlib.patch",
    ],
    sha256 = "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23",
    strip_prefix = "zlib-1.3.1",
    urls = ["https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies")

rules_proto_dependencies()

load("@rules_proto//proto:toolchains.bzl", "rules_proto_toolchains")

rules_proto_toolchains()

# rules_python is loaded by protobuf_deps, but the dependencies of rules_python are not
# we need to load it here to ensure com_google_protobuf could be used.
load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()

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

all_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

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

http_archive(
    name = "rules_oci",
    sha256 = "d007e6c96eb62c88397b68f329e4ca56e0cfe31204a2c54b0cb17819f89f83c8",
    strip_prefix = "rules_oci-2.0.0",
    url = "https://github.com/bazel-contrib/rules_oci/releases/download/v2.0.0/rules_oci-v2.0.0.tar.gz",
)

load("@rules_oci//oci:dependencies.bzl", "rules_oci_dependencies")

rules_oci_dependencies()

load("@rules_oci//oci:repositories.bzl", "oci_register_toolchains")

oci_register_toolchains(name = "oci")

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

dockerfile_image(
    name = "xfsprogs_image",
    dockerfile = "//dockerfiles/test_images/xfsprogs_image:Dockerfile",
    visibility = ["//visibility:public"],
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

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

container_pull(
    name = "buildbuddy_go_image_base",
    digest = "sha256:388145607c79313a1e49b783a7ee71e4ef3df31d87c45adb46bfb9b257b643d1",
    registry = "gcr.io",
    repository = "distroless/cc-debian12",
)

# Base image that can be used to build images that are capable of running the Bazel binary.
container_pull(
    name = "bazel_image_base",
    digest = "sha256:8bb82ccf73085b71159ce05d2cc6030cbaa927b403c04774f0b22f37ab4fd78a",
    registry = "gcr.io",
    repository = "distroless/java17-debian12",
)

load("@rules_oci//oci:pull.bzl", "oci_pull")

oci_pull(
    name = "buildbuddy_go_oci_image_base",
    digest = "sha256:388145607c79313a1e49b783a7ee71e4ef3df31d87c45adb46bfb9b257b643d1",
    image = "gcr.io/distroless/cc-debian12",
    platforms = ["linux/amd64"],
)

oci_pull(
    name = "bazel_oci_image_base",
    digest = "sha256:8bb82ccf73085b71159ce05d2cc6030cbaa927b403c04774f0b22f37ab4fd78a",
    image = "gcr.io/distroless/java17-debian12",
    platforms = ["linux/amd64"],
)

# BuildBuddy Toolchain
# Keep up-to-date with docs/rbe-setup.md and docs/rbe-github-actions.md
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "baa9af1b9fcc96d18ac90a4dd68ebd2046c8beb76ed89aea9aabca30959ad30c",
    strip_prefix = "buildbuddy-toolchain-287d6042ad151be92de03c83ef48747ba832c4e2",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/287d6042ad151be92de03c83ef48747ba832c4e2.tar.gz"],
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

# esbuild (for bundling JS)

load("@build_bazel_rules_nodejs//toolchains/esbuild:esbuild_repositories.bzl", "esbuild_repositories")

esbuild_repositories(npm_repository = "npm")

# SWC (for transpiling TS -> JS)
http_archive(
    name = "aspect_rules_swc",
    sha256 = "d63d7b283249fa942f78d2716ecff3edbdc10104ee1b9a6b9464ece471ef95ea",
    strip_prefix = "rules_swc-2.0.0",
    url = "https://github.com/aspect-build/rules_swc/releases/download/v2.0.0/rules_swc-v2.0.0.tar.gz",
)

load("@aspect_rules_swc//swc:dependencies.bzl", "rules_swc_dependencies")

rules_swc_dependencies()

load("@aspect_rules_swc//swc:repositories.bzl", "swc_register_toolchains")

swc_register_toolchains(
    name = "swc",
    swc_version = "v1.3.78",
)

# Web testing

http_archive(
    name = "io_bazel_rules_webtesting",
    sha256 = "3e25ac044ed409545214cf8b013fa7255ccf1d2fa027b0d57a3fcc7d732da667",
    strip_prefix = "rules_webtesting-9e9361ba887a3b687f537c02409b690b62fecdfe",
    urls = [
        "https://github.com/bazelbuild/rules_webtesting/archive/9e9361ba887a3b687f537c02409b690b62fecdfe.tar.gz",
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
    sha256 = "ed2b625ceeca0ebacf413972c33acbeb65a6c6b94d0c6434f1bb006cd4904ede",
    url = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem",
)

register_toolchains(
    "//toolchains:sh_toolchain",
    "//toolchains:ubuntu_cc_toolchain",
)

http_archive(
    name = "musl_toolchains",
    sha256 = "26cacffab74e10f0840c83b0be9193dc6deccfbc8ec1cf6f8362dc61d0057fa1",
    url = "https://github.com/bazel-contrib/musl-toolchain/releases/download/v0.1.15/musl_toolchain-v0.1.15.tar.gz",
)

load("@musl_toolchains//:repositories.bzl", "load_musl_toolchains")

load_musl_toolchains(
    extra_target_compatible_with = [
        "@//toolchains:musl_on",
    ],
)

load("@musl_toolchains//:toolchains.bzl", "register_musl_toolchains")

register_musl_toolchains()
