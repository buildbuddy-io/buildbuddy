load("@bazel_skylib//lib:modules.bzl", "modules")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# The podman version used in tests and distributed with the buildbuddy executor image.
# When changing this version, a new release of podman-static may be needed.
# See dockerfiles/executor_image/README.md for instructions.
# The checksums below will also need to be updated.
PODMAN_VERSION = "v5.7.0"
PODMAN_STATIC_SHA256_AMD64 = "6a1c06b78d7dad15d8d7155a180874939a04bd39ce2f64726c7f11142ab7aa7d"
PODMAN_STATIC_SHA256_ARM64 = "703ffad8972aa2db70a173c80804a88185e6c2dc8a88a247a8ebffeac424b0ba"

# Manually created
def install_static_dependencies(workspace_name = "buildbuddy"):
    http_archive(
        name = "com_github_buildbuddy_io_protoc_gen_protobufjs",
        integrity = "sha256-0Glb6xE/18H8cVTsfolRVmAOX1Rgrm3XAkJMT1W1dkI=",
        urls = ["https://github.com/buildbuddy-io/protoc-gen-protobufjs/releases/download/v0.0.14/protoc-gen-protobufjs-v0.0.14.tar.gz"],
    )
    http_archive(
        name = "com_github_sluongng_nogo_analyzer",
        sha256 = "a74a5e44751d292d17bd879e5aa8b40baa94b5dc2f043df1e3acbb3e23ead073",
        strip_prefix = "nogo-analyzer-0.0.2",
        urls = [
            "https://github.com/sluongng/nogo-analyzer/archive/refs/tags/v0.0.2.tar.gz",
        ],
    )

    http_archive(
        name = "com_github_firecracker_microvm_firecracker",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "firecracker", srcs = ["firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["jailer-{release}"])',
        ]).format(release = "v1.13.0-with_clock_reset_patch-20251029-d33011c6788153a8a601e1b7000466c1ff1ecfc7"),
        sha256 = "d08f5245fcb84c59bdb060385e3058f4667765afbea5c3cbfcb969d570d624bf",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/firecracker/firecracker-v1.13.0-with_clock_reset_patch.tgz"],
    )
    http_archive(
        name = "com_github_firecracker_microvm_firecracker_arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "firecracker", srcs = ["release-{release}/firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["release-{release}/jailer-{release}"])',
        ]).format(release = "v1.11.0-aarch64"),
        sha256 = "4b98f7cd669a772716fd1bef59c75188ba05a683bc0759ee4169eb351274fcb0",
        urls = ["https://github.com/firecracker-microvm/firecracker/releases/download/v1.11.0/firecracker-v1.11.0-aarch64.tgz"],
    )

    http_file(
        name = "io_bazel_bazel-6.5.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-darwin-arm64"],
        sha256 = "c6b6dc17efcdf13fba484c6fe0b6c3361b888ae7b9573bc25a2dbe8c502448eb",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.5.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-darwin-x86_64"],
        sha256 = "bbf9c2c03bac48e0514f46db0295027935535d91f6d8dcd960c53393559eab29",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.5.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-linux-arm64"],
        sha256 = "5afe973cadc036496cac66f1414ca9be36881423f576db363d83afc9084c0c2f",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.5.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-linux-x86_64"],
        sha256 = "a40ac69263440761199fcb8da47ad4e3f328cbe79ffbf4ecc14e5ba252857307",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.7.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.7.1/bazel-7.7.1-darwin-arm64"],
        sha256 = "fe8a1ee9064e94afae075c0dd4efb453db9c1373b9df12fecbff8479d408eb08",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.7.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.7.1/bazel-7.7.1-darwin-x86_64"],
        sha256 = "8582aea5ee2d8d0448bbda10fd7034734db1a21cbe4ea351a10012b969aa5d31",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.7.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.7.1/bazel-7.7.1-linux-arm64"],
        sha256 = "71df04ec724f1b577f1f47ec9a6b81d13f39683f6c3215cacf45fdaf40b2c5c1",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.7.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.7.1/bazel-7.7.1-linux-x86_64"],
        sha256 = "115a1b62be95f29e5821d4dddffba1b058905a48019b499919c285e7f708d5e2",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.3.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.3.1/bazel-8.3.1-darwin-arm64"],
        sha256 = "0cac3a67dc5429c68272dc6944104952e9e4cf84b29d126a5ff3fbaa59045217",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.3.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.3.1/bazel-8.3.1-darwin-x86_64"],
        sha256 = "0425c87f4d540fdf292b5a77d70401fb5e7a52165956a9e522bebe46acb6e1b2",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.3.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.3.1/bazel-8.3.1-linux-arm64"],
        sha256 = "52bb74f0880ee87b17e3a1aa41257d0dba9e9cb92df4e83f8f6c656a46df152d",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.3.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.3.1/bazel-8.3.1-linux-x86_64"],
        sha256 = "17247e8a84245f59d3bc633d0cfe0a840992a7760a11af1a30012d03da31604c",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.4.2-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.4.2/bazel-8.4.2-darwin-arm64"],
        sha256 = "45e9388abf21d1107e146ea366ad080eb93cb6a5f3a4a3b048f78de0bc3faffa",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.4.2-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.4.2/bazel-8.4.2-darwin-x86_64"],
        sha256 = "ce73346274c379f77880db8bd8b9c8569885fe56f19386173760949da9078df0",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.4.2-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.4.2/bazel-8.4.2-linux-arm64"],
        sha256 = "58e6042fc54f3bf5704452b579f575ae935817d4b842a76123f05fae6b1f9a83",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.4.2-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.4.2/bazel-8.4.2-linux-x86_64"],
        sha256 = "4dc8e99dfa802e252dac176d08201fd15c542ae78c448c8a89974b6f387c282c",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.0/bazel-8.5.0-darwin-arm64"],
        sha256 = "a89f446641ab1cce603691cb7030865d1fb014e260ee5710615516e3cacd2414",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.0/bazel-8.5.0-darwin-x86_64"],
        sha256 = "84e1b822b6d076151a9a5b7a962e8230f565a17207196056bfe35303077b8147",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.0/bazel-8.5.0-linux-arm64"],
        sha256 = "0c455abf42814ac53539ddd8147249a11b9e05c7dc83dbd6c8dfac1aec243d85",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.0/bazel-8.5.0-linux-x86_64"],
        sha256 = "18255229d933b8da10151bdef223a302744296b09af8af1988c93faa1ea3c71f",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.0rc3-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.0rc3/bazel-9.0.0rc3-darwin-arm64"],
        sha256 = "fa1d0189a8ab1a0c20a402e9d2a9480720e82364a47b461f8d6c15dcb8632f56",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.0rc3-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.0rc3/bazel-9.0.0rc3-darwin-x86_64"],
        sha256 = "eb6973ad87d2d0b85b0d35752f4bc30de02fa190aa6ca76c30270f55c0427723",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.0rc3-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.0rc3/bazel-9.0.0rc3-linux-arm64"],
        sha256 = "a957c3a39813ff3f09103165370bcfbeb48e62ae512547024b68c6541e2c538b",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.0rc3-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.0rc3/bazel-9.0.0rc3-linux-x86_64"],
        sha256 = "de62d93b6d58b344c382cb374f940117e75d9a37e047feffd1803e29022a507d",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-darwin-amd64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-darwin-amd64"],
        sha256 = "0af019eeb642fa70744419d02aa32df55e6e7a084105d49fb26801a660aa56d3",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-darwin-arm64"],
        sha256 = "b13dd89c6ecd90944ca3539f5a2c715a18f69b7458878c471a902a8e482ceb4b",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-linux-amd64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-linux-amd64"],
        sha256 = "fd8fdff418a1758887520fa42da7e6ae39aefc788cf5e7f7bb8db6934d279fc4",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-linux-arm64"],
        sha256 = "4c8d966e40ac2c4efcc7f1a5a5cceef2c0a2f16b957e791fa7a867cce31e8fcb",
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux",
        sha256 = "3fd19c602f2b11969ad563d4d4855c9147cf13c34238537c1e434097a11aa6b7",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-v5.15-3fd19c602f2b11969ad563d4d4855c9147cf13c34238537c1e434097a11aa6b7"],
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux-6.1",
        sha256 = "221765c1c163d7f4687c0fba573c47a17ada6cbe4063c16e6205fabc7066fd15",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-x86_64-v6.1-221765c1c163d7f4687c0fba573c47a17ada6cbe4063c16e6205fabc7066fd15"],
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux-arm64",
        sha256 = "15e7206a252b19807dcba81f42c61b42923ca706ce85a5882248162cf1317d70",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-aarch64-v6.1-15e7206a252b19807dcba81f42c61b42923ca706ce85a5882248162cf1317d70"],
        executable = True,
    )

    http_file(
        name = "org_llvm_llvm_clang-format_linux-x86_64",
        executable = True,
        integrity = "sha256-BQxgAlbiJeq+lgjSj0kv6Gc8bn9d6sWcbalzIjx2TWw=",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/clang-format/clang-format-15_linux-x86_64"],
    )

    http_file(
        name = "org_llvm_llvm_clang-format_macos-x86_64",
        executable = True,
        integrity = "sha256-lxFvZNl/socLSqKXWLuo+w/n87HtikvBL6qSfs/ewZY=",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/clang-format/clang-format-15_darwin-x86_64"],
    )

    http_file(
        name = "com_github_buildbuddy_io_podman_static_podman-linux-amd64",
        urls = ["https://github.com/buildbuddy-io/podman-static/releases/download/buildbuddy-{podman_version}/podman-linux-amd64.tar.gz".format(podman_version = PODMAN_VERSION)],
        sha256 = PODMAN_STATIC_SHA256_AMD64,
    )

    http_file(
        name = "com_github_buildbuddy_io_podman_static_podman-linux-arm64",
        urls = ["https://github.com/buildbuddy-io/podman-static/releases/download/buildbuddy-{podman_version}/podman-linux-arm64.tar.gz".format(podman_version = PODMAN_VERSION)],
        sha256 = PODMAN_STATIC_SHA256_ARM64,
    )

    # NOTE: crun 1.16.1 has a double-free bug. Before upgrading, be sure the
    # release includes a fix for https://github.com/containers/crun/issues/1537
    http_file(
        name = "com_github_containers_crun_crun-linux-amd64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-amd64-disable-systemd"],
        sha256 = "03fd3ec6a7799183eaeefba5ebd3f66f9b5fb41a5b080c196285879631ff5dc1",
        downloaded_file_path = "crun",
        executable = True,
    )
    http_file(
        name = "com_github_containers_crun_crun-linux-arm64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-arm64-disable-systemd"],
        sha256 = "1bd840c95e9ae8edc25654dcf2481309724b9ff18ce95dbcd2535da9b026a47d",
        downloaded_file_path = "crun",
        executable = True,
    )

    # busybox static builds (see tools/build_busybox.sh)
    http_file(
        name = "net_busybox_busybox-linux-amd64",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/busybox/busybox-1.36.1_linux-amd64"],
        sha256 = "f1c20582a7efb70382d63ec71fbeb6f422d7338592ff28ffe0bdff55c839a94c",
        downloaded_file_path = "busybox",
        executable = True,
    )
    http_file(
        name = "net_busybox_busybox-linux-arm64",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/busybox/busybox-1.36.1_linux-arm64"],
        sha256 = "19601547c937e9636f64ae2dbd7d2d318dc5bbd58f7d5624bfa452f22cfe33d1",
        downloaded_file_path = "busybox",
        executable = True,
    )

    http_file(
        name = "com_github_krallin_tini_tini-linux-amd64",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64"],
        sha256 = "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c",
        downloaded_file_path = "tini",
        executable = True,
    )
    http_file(
        name = "com_github_krallin_tini_tini-linux-arm64",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini-arm64"],
        sha256 = "07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81",
        downloaded_file_path = "tini",
        executable = True,
    )

    http_archive(
        name = "com_github_googlecloudplatform_docker-credential-gcr-linux-amd64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "docker-credential-gcr.bin", srcs = ["docker-credential-gcr"])',
        ]),
        urls = ["https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.30/docker-credential-gcr_linux_amd64-2.1.30.tar.gz"],
        sha256 = "d5c90c03d90271873a8619b1f73023a0266ae3fc91965ce9c81d7903e4b54eb6",
    )
    http_archive(
        name = "com_github_googlecloudplatform_docker-credential-gcr-linux-arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "docker-credential-gcr.bin", srcs = ["docker-credential-gcr"])',
        ]),
        urls = ["https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.30/docker-credential-gcr_linux_arm64-2.1.30.tar.gz"],
        sha256 = "ac9c0237e40505f09796c2bf8a90377246a6fd3cb65e6eada77009ae0f2d3b00",
    )

    http_archive(
        name = "com_github_rootless_containers_rootlesskit-linux-amd64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            # 'filegroup(name = "rootlessctl.bin", srcs = ["rootlessctl"])',
            'filegroup(name = "rootlesskit.bin", srcs = ["rootlesskit"])',
        ]),
        urls = ["https://github.com/rootless-containers/rootlesskit/releases/download/v2.3.5/rootlesskit-x86_64.tar.gz"],
        sha256 = "118208e25becd144ee7317c172fc9decce7b16174d5c1bbf80f1d1d0eacc6b5f",
    )
    http_archive(
        name = "com_github_rootless_containers_rootlesskit-linux-arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            # 'filegroup(name = "rootlessctl.bin", srcs = ["rootlessctl"])',
            'filegroup(name = "rootlesskit.bin", srcs = ["rootlesskit"])',
        ]),
        urls = ["https://github.com/rootless-containers/rootlesskit/releases/download/v2.3.5/rootlesskit-aarch64.tar.gz"],
        sha256 = "478c14c3195bf989cd9a8e6bd129d227d5d88f1c11418967ffdc84a0072cc7a2",
    )

    http_archive(
        name = "com_github_containerd_containerd-linux-amd64",
        strip_prefix = "bin",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "containerd.bin", srcs = ["containerd"])',
            'filegroup(name = "containerd-shim-runc-v2.bin", srcs = ["containerd-shim-runc-v2"])',
            'filegroup(name = "ctr.bin", srcs = ["ctr"])',
        ]),
        urls = ["https://github.com/containerd/containerd/releases/download/v2.2.0/containerd-2.2.0-linux-amd64.tar.gz"],
        sha256 = "b9626a94ab93b00bcbcbf13d98deef972c6fb064690e57940632df54ad39ee71",
    )
    http_archive(
        name = "com_github_containerd_containerd-linux-arm64",
        strip_prefix = "bin",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "containerd.bin", srcs = ["containerd"])',
            'filegroup(name = "containerd-shim-runc-v2.bin", srcs = ["containerd-shim-runc-v2"])',
            'filegroup(name = "ctr.bin", srcs = ["ctr"])',
        ]),
        urls = ["https://github.com/containerd/containerd/releases/download/v2.2.0/containerd-2.2.0-linux-arm64.tar.gz"],
        sha256 = "8805c2123d3b7c7ee2030e9f8fc07a1167d8a3f871d6a7d7ec5d1deb0b51a4a7",
    )

    http_archive(
        name = "cloudprober",
        build_file_content = "exports_files([\"cloudprober\"])",
        sha256 = "84db0c196fc8581ff4f6c9339036967bcf9b3a7cbe4d8d753067f474da9ca167",
        strip_prefix = "cloudprober-v0.11.9-linux-x86_64",
        urls = ["https://github.com/cloudprober/cloudprober/releases/download/v0.11.9/cloudprober-v0.11.9-linux-x86_64.zip"],
    )

    http_file(
        name = "com_github_opencontainers_runc_runc-linux-amd64",
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.3.3/runc.amd64"],
        sha256 = "8781ab9f71c12f314d21c8e85f13ca1a82d90cf475aa5131a7b543fcc5487543",
        downloaded_file_path = "runc",
        executable = True,
    )
    http_file(
        name = "com_github_opencontainers_runc_runc-linux-arm64",
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.3.3/runc.arm64"],
        sha256 = "3c9a8e9e6dafd00db61f4611692447ebab4a56388bae4f82192aed67b66df712",
        downloaded_file_path = "runc",
        executable = True,
    )

install_static_dependencies_ext = modules.as_extension(install_static_dependencies)
