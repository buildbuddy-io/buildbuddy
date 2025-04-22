workspace(
    name = "buildbuddy",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# Load core rulesets before invoking any dependency macros to ensure that the
# versions listed below are actually honored - the last repo from the first
# block delimited by loads wins.

# Bazel platforms
http_archive(
    name = "platforms",
    integrity = "sha256-KXQuhydYCbXlmNwvBNhpYMx6VbMGfZciHJq7yZJr/w8=",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.11/platforms-0.0.11.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.11/platforms-0.0.11.tar.gz",
    ],
)

http_archive(
    name = "bazel_features",
    sha256 = "cec7fbc7bce6597cf2e83e01ddd9328a1bb057dc1a3092745238f49d3301ab5a",
    strip_prefix = "bazel_features-1.12.0",
    url = "https://github.com/bazel-contrib/bazel_features/releases/download/v1.12.0/bazel_features-v1.12.0.tar.gz",
)

http_archive(
    name = "bazel_skylib",
    sha256 = "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
    ],
)

http_archive(
    name = "rules_cc",
    sha256 = "abc605dd850f813bb37004b77db20106a19311a96b2da1c92b789da529d28fe1",
    strip_prefix = "rules_cc-0.0.17",
    urls = ["https://github.com/bazelbuild/rules_cc/releases/download/0.0.17/rules_cc-0.0.17.tar.gz"],
)

http_archive(
    name = "rules_java",
    sha256 = "a64ab04616e76a448c2c2d8165d836f0d2fb0906200d0b7c7376f46dd62e59cc",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/8.6.2/rules_java-8.6.2.tar.gz",
    ],
)

http_archive(
    name = "rules_python",
    integrity = "sha256-LMJrvVOFTOt23UKoNLEALNS6f43zVEDPA0guBFr/wkQ=",
    strip_prefix = "rules_python-1.3.0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/1.3.0/rules_python-1.3.0.tar.gz",
)

http_archive(
    name = "rules_shell",
    sha256 = "d8cd4a3a91fc1dc68d4c7d6b655f09def109f7186437e3f50a9b60ab436a0c53",
    strip_prefix = "rules_shell-0.3.0",
    url = "https://github.com/bazelbuild/rules_shell/releases/download/v0.3.0/rules_shell-v0.3.0.tar.gz",
)

# Proto rules

http_archive(
    name = "com_google_protobuf",
    integrity = "sha256-6bmsGRCxBBBlg5hQYDyvNuKdPT0jDd9SvRN3jdMbkEY=",
    strip_prefix = "protobuf-29.3",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protobuf-29.3.zip"],
)

http_archive(
    name = "rules_proto",
    sha256 = "14a225870ab4e91869652cfd69ef2028277fc1dc4910d65d353b62d6e0ae21f4",
    strip_prefix = "rules_proto-7.1.0",
    url = "https://github.com/bazelbuild/rules_proto/releases/download/7.1.0/rules_proto-7.1.0.tar.gz",
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

# Packaging rules

http_archive(
    name = "rules_pkg",
    integrity = "sha256-0gyVGWDtd8t7NBwqWUiFNOSU1a0dMMSBjHNtV3cqn+8=",
    url = "https://github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
)

load("@bazel_features//:deps.bzl", "bazel_features_deps")

bazel_features_deps()

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

load("@rules_java//java:rules_java_deps.bzl", "rules_java_dependencies")

rules_java_dependencies()

# note that the following line is what is minimally required from protobuf for the java rules
# consider using the protobuf_deps() public API from @com_google_protobuf//:protobuf_deps.bzl
load("@com_google_protobuf//bazel/private:proto_bazel_features.bzl", "proto_bazel_features")  # buildifier: disable=bzl-visibility

proto_bazel_features(name = "proto_bazel_features")

# register toolchains
load("@rules_java//java:repositories.bzl", "rules_java_toolchains")

rules_java_toolchains()

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()

load("@rules_shell//shell:repositories.bzl", "rules_shell_dependencies", "rules_shell_toolchains")

rules_shell_dependencies()

rules_shell_toolchains()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies")

rules_proto_dependencies()

load("@rules_proto//proto:toolchains.bzl", "rules_proto_toolchains")

rules_proto_toolchains()

# Go

# keep in sync with go.mod
http_archive(
    name = "io_bazel_rules_go",
    integrity = "sha256-t493RY53Fi9FtFZNayC2+S9WQx7VnqqrCeeBnR2FAxM=",
    patch_args = ["-p1"],
    patches = [
        # https://github.com/bazel-contrib/rules_go/pull/4264
        # TODO(sluongng): remove when v0.54.0 is released
        "//buildpatches:rules_go.patch",
    ],
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    integrity = "sha256-XYDmKnAxTznMdkwcPqqADFk2yfHqkWJQBiJ85NIM0IY=",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:gazelle.patch",
    ],
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz",
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
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_nogo", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

GO_SDK_VERSION = "1.24.2"

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

go_register_nogo(
    nogo = "@//:vet",
)

go_register_toolchains()

gazelle_dependencies(
    go_env = {
        "GOPROXY": "https://proxy.golang.org|direct",
    },
)

# Version taken from
# https://github.com/bazelbuild/bazel-central-registry/blob/main/modules/googleapis/0.0.0-20241220-5e258e33/source.json
# So that we can use the same version of googleapis as the one used in bzlmod.
http_archive(
    name = "googleapis",
    integrity = "sha256-ftfNAEA+XKYX5I+kRRNhXnnfyBert9Rgq1WQ6sJ0Pvk=",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:googleapis.patch",
    ],
    strip_prefix = "googleapis-5e258e334154da04dcd0a567a61ac21518cac81b",
    urls = [
        "https://github.com/googleapis/googleapis/archive/5e258e334154da04dcd0a567a61ac21518cac81b.tar.gz",
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

# JS

http_archive(
    name = "aspect_rules_js",
    sha256 = "d66f8abf914a0454a69181b7b17acaae56d7b0e2784cb26b40cb3273c4d836d1",
    strip_prefix = "rules_js-2.2.0",
    url = "https://github.com/aspect-build/rules_js/releases/download/v2.2.0/rules_js-v2.2.0.tar.gz",
)

load("@aspect_rules_js//js:repositories.bzl", "rules_js_dependencies")

rules_js_dependencies()

load("@aspect_rules_js//js:toolchains.bzl", "rules_js_register_toolchains")

rules_js_register_toolchains(node_version = "18.20.3")

load("@aspect_rules_js//npm:repositories.bzl", "npm_translate_lock")

npm_translate_lock(
    name = "npm",
    npmrc = "//:.npmrc",
    patch_args = {
        "@protobufjs/inquire": [
            "-p1",
            "--binary",
        ],
    },
    patches = {
        "@protobufjs/inquire": [
            # Patch out use of eval to satisfy a strict CSP.
            # https://github.com/protobufjs/protobuf.js/issues/593
            "//buildpatches:protobuf.js_inquire.patch",
        ],
    },
    pnpm_lock = "//:pnpm-lock.yaml",
    verify_node_modules_ignored = "//:.bazelignore",
)

load("@npm//:repositories.bzl", "npm_repositories")

npm_repositories()

# TS

http_archive(
    name = "aspect_rules_ts",
    sha256 = "d584e4bc80674d046938563678117d17df962fe105395f6b1efe2e8a248b8100",
    strip_prefix = "rules_ts-3.5.1",
    url = "https://github.com/aspect-build/rules_ts/releases/download/v3.5.1/rules_ts-v3.5.1.tar.gz",
)

load("@aspect_rules_ts//ts:repositories.bzl", "rules_ts_dependencies")

rules_ts_dependencies(
    # TODO: Remove after the next aspect_rules_ts update.
    ts_integrity = "sha512-aJn6wq13/afZp/jT9QZmwEjDqqvSGp1VT5GVg+f/t6/oVyrgXM6BY1h9BRh/O5p3PlUPAe+WuiEZOmb/49RqoQ==",
    ts_version_from = "//:package.json",
)

# esbuild

http_archive(
    name = "aspect_rules_esbuild",
    sha256 = "550e33ddeb86a564b22b2c5d3f84748c6639b1b2b71fae66bf362c33392cbed8",
    strip_prefix = "rules_esbuild-0.21.0",
    url = "https://github.com/aspect-build/rules_esbuild/releases/download/v0.21.0/rules_esbuild-v0.21.0.tar.gz",
)

load("@aspect_rules_esbuild//esbuild:dependencies.bzl", "rules_esbuild_dependencies")

rules_esbuild_dependencies()

load("@aspect_rules_esbuild//esbuild:repositories.bzl", "LATEST_ESBUILD_VERSION", "esbuild_register_toolchains")

esbuild_register_toolchains(
    name = "esbuild",
    esbuild_version = LATEST_ESBUILD_VERSION,
)

# jasmine

http_archive(
    name = "aspect_rules_jasmine",
    sha256 = "0d2f9c977842685895020cac721d8cc4f1b37aae15af46128cf619741dc61529",
    strip_prefix = "rules_jasmine-2.0.0",
    url = "https://github.com/aspect-build/rules_jasmine/releases/download/v2.0.0/rules_jasmine-v2.0.0.tar.gz",
)

load("@aspect_rules_jasmine//jasmine:dependencies.bzl", "rules_jasmine_dependencies")

rules_jasmine_dependencies()

# swc

http_archive(
    name = "aspect_rules_swc",
    sha256 = "d9ed410eb6a5c605345d872f0c4c50138bda3c572db2aed1796cf397fa361986",
    strip_prefix = "rules_swc-2.3.0",
    url = "https://github.com/aspect-build/rules_swc/releases/download/v2.3.0/rules_swc-v2.3.0.tar.gz",
)

load("@aspect_rules_swc//swc:dependencies.bzl", "rules_swc_dependencies")

rules_swc_dependencies()

load("@aspect_rules_swc//swc:repositories.bzl", "swc_register_toolchains")

swc_register_toolchains(
    name = "swc",
    swc_version = "v1.3.78",
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

# Docker

http_archive(
    name = "io_bazel_rules_docker",
    integrity = "sha256-ZIUZu2mLsls7gXUqLObaGizFZlNDa0jGkdzpuNahgGE=",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:rules_docker.patch",
    ],
    strip_prefix = "rules_docker-b44cc958e61c3192c57fed7aef78c8567d757a70",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/b44cc958e61c3192c57fed7aef78c8567d757a70.tar.gz"],
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

# Kubernetes

http_archive(
    name = "io_bazel_rules_k8s",
    integrity = "sha256-51xa8wL5mhoGlsvgTEm4NRXsfCVgX5lR8a1/evO/pBY=",
    patch_args = ["-p1"],
    patches = [
        "//buildpatches:rules_k8s.patch",
    ],
    strip_prefix = "rules_k8s-554dc69933461ae2fa4fefcc46d09d5784832e6c",
    urls = ["https://github.com/bazelbuild/rules_k8s/archive/554dc69933461ae2fa4fefcc46d09d5784832e6c.tar.gz"],
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
    name = "rbe-ubuntu22-04_image",
    dockerfile = "//dockerfiles/rbe-ubuntu22-04:Dockerfile",
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
    digest = "sha256:dc7acdb6300eaa99ae93621b0f033237ae1284fdd5ab323b4d90ba8359c55854",
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

oci_pull(
    name = "busybox",
    digest = "sha256:c230832bd3b0be59a6c47ed64294f9ce71e91b327957920b6929a0caa8353140",
    image = "mirror.gcr.io/library/busybox:1.36.1",
    platforms = ["linux/amd64"],
)

# BuildBuddy Toolchain
# Keep up-to-date with
# - docs/rbe-setup.md
# - docs/rbe-github-actions.md
# - tools/dev_qa.py
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    integrity = "sha256-e6gcgLHmJHvxCNNbCSQ4OrX8FbGn8TiS7XSVphM1ZU8=",
    strip_prefix = "buildbuddy-toolchain-badf8034b2952ec613970a27f24fb140be7eaf73",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/badf8034b2952ec613970a27f24fb140be7eaf73.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    container_image = UBUNTU20_04_IMAGE,
    # This is the MSVC available on Github Action win22 image
    # https://github.com/actions/runner-images/blob/win22/20250303.1/images/windows/Windows2022-Readme.md
    msvc_edition = "Enterprise",
    msvc_release = "2022",
    # From 'Microsoft Visual C++ 2022 Minimum Runtime' for x64 architecture
    # https://github.com/actions/runner-images/blob/win22/20250303.1/images/windows/Windows2022-Readme.md#microsoft-visual-c
    msvc_version = "14.43.34808",
)

http_archive(
    name = "cloudprober",
    build_file_content = "exports_files([\"cloudprober\"])",
    sha256 = "0a824a6e224d9810514f4a2f4a13f09488672ad483bb0e978c16d8a6b3372625",
    strip_prefix = "cloudprober-v0.11.2-ubuntu-x86_64",
    urls = ["https://github.com/google/cloudprober/releases/download/v0.11.2/cloudprober-v0.11.2-ubuntu-x86_64.zip"],
)

# Web testing

http_archive(
    name = "rules_webtesting",
    integrity = "sha256-wJV/ZIAEYtzoEynx+f6NYA1ilAIrK6ppbUuaeI0+j3Y=",
    strip_prefix = "rules_webtesting-7a1c88f61e35ee5ce0892ae24e2aa2a3106cbfed",
    urls = [
        "https://github.com/bazelbuild/rules_webtesting/archive/7a1c88f61e35ee5ce0892ae24e2aa2a3106cbfed.tar.gz",
    ],
)

load("@rules_webtesting//web/versioned:browsers-0.3.4.bzl", "browser_repositories")

browser_repositories()

register_toolchains(
    "@buildbuddy_toolchain//:all",
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
