load("@bazel_skylib//lib:modules.bzl", "modules")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# The podman version used in tests and distributed with the buildbuddy executor image.
# When changing this version, a new release of podman-static may be needed.
# See dockerfiles/executor_image/README.md for instructions.
# The checksums below will also need to be updated.
PODMAN_VERSION = "v5.5.0"
PODMAN_STATIC_SHA256_AMD64 = "8ce959cd2b0ea68ae8ac7ccbb181cad59504086d54d4c9521954ee49dae013eb"
PODMAN_STATIC_SHA256_ARM64 = "ef1e84ab80ee5d78d4d2e59e128ff963038f39e1e4259a83e08d7c8f85faf90d"

# Manually created
def install_static_dependencies(workspace_name = "buildbuddy"):
    http_archive(
        name = "com_github_buildbuddy_io_protoc_gen_protobufjs",
        integrity = "sha256-VL3iXrCZj9OiYrads5Ve6uHjppfc5i8mHexOrVvpOI4=",
        urls = ["https://github.com/buildbuddy-io/protoc-gen-protobufjs/releases/download/v0.0.13/protoc-gen-protobufjs-v0.0.13.tar.gz"],
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
        ]).format(release = "v1.11.0-with_move_clock_reset_patch-20251013-b0b60411d22e5be83016474705f45504554db659"),
        sha256 = "12814a607e18b2e844494df88b08f3b6195f6e890add6a156d5fb54ead2ca10b",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/firecracker/firecracker-v1.11.0-with_move_clock_reset_patch.tgz"],
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
        name = "com_github_redis_redis-redis-server-v6.2.1-linux-x86_64",
        executable = True,
        sha256 = "6d9c268fa0f696c3fc71b3656936c777d02b3b1c6637674ac3173facaefa4a77",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.1-linux-x86_64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.1-linux-arm64",
        executable = True,
        sha256 = "c381660c79dcbe4835a243bab8a0d3c3ee5dc502c1b4a47f1761bd8f9e7be240",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.1-linux-arm64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.6-darwin-arm64",
        executable = True,
        sha256 = "a70261f7a3f455a9a7c9d845299d487a86c1faef2af1605b94f39db44c098e69",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.6-darwin-arm64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.6-darwin-x86_64",
        executable = True,
        sha256 = "398bcf2be83a30249ec81259c87a7fc5c3bf511ff9b222ed517d8e52c99733c6",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.6-darwin-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-darwin-arm64"],
        sha256 = "d3ce116379b835e2a0ae16b924a485c48330db434b192f090c8f875238e584cb",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-darwin-x86_64"],
        sha256 = "fe01824013184899386a4807435e38811949ca13f46713e7fc39c70fa1528a17",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-linux-arm64"],
        sha256 = "dcad413da286ac1d3f88e384ff05c2ed796f903be85b253591d170ce258db721",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-linux-x86_64"],
        sha256 = "973e213b1e9207ccdd3ea4730c0f92cbef769ec112ac2b84980583220d8db845",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.2.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.2.1/bazel-6.2.1-darwin-arm64"],
        sha256 = "0e4409d3243bf04bb709d3f1cc8a32ec0c36475c6d2aeda8475a213c40470793",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.2.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.2.1/bazel-6.2.1-darwin-x86_64"],
        sha256 = "dd69512405d7a07c14ee2b33c8e1cb434b2eac203b3d46e17e7acb797608db22",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.2.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.2.1/bazel-6.2.1-linux-arm64"],
        sha256 = "98d17ba59885957fe0dda423a52cfc3edf91176d4a7b3bdc5b573975a3785e1e",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.2.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.2.1/bazel-6.2.1-linux-x86_64"],
        sha256 = "cdf349dc938b1f11db5a7172269d66e91ce18c0ca134b38bb3997a3e3be782b8",
        executable = True,
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
        name = "io_bazel_bazel-7.1.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-darwin-arm64"],
        sha256 = "fb5e7bc62fc3c8f2511e3b64d795296444129b26f13d8dece4d4cee1845b758f",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.1.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-darwin-x86_64"],
        sha256 = "52ad8d57c22e4f873c724473a09ecfd98966c3a2950e102a7bd7e8c612b8001c",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.1.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-linux-arm64"],
        sha256 = "b8cb5f842ce457606fbff3dba7a47e973f72bba0af1b7575ae500ca5a3d44282",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.1.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-linux-x86_64"],
        sha256 = "62d62c699c1eb9f9be6a88030912a54d19fe45ae29329c7e5c53aba787492522",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.6.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.6.1/bazel-7.6.1-darwin-arm64"],
        sha256 = "45cca81a839d7495258b19ee8371c7b891f350586ef37b9940f7b531eb654cc8",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.6.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.6.1/bazel-7.6.1-darwin-x86_64"],
        sha256 = "3b007e7ce2281408b99dbba11b35d2ae0191de1330fae49dc632077e93edf78e",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.6.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.6.1/bazel-7.6.1-linux-arm64"],
        sha256 = "2d86d36db0c9af15747ff02a80e6db11a45d68f868ea8f62f489505c474f0099",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-7.6.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.6.1/bazel-7.6.1-linux-x86_64"],
        sha256 = "ac6249d1192aea9feaf49dfee2ab50c38cee2454b00cf29bbec985a11795c025",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.1.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.1.1/bazel-8.1.1-darwin-arm64"],
        sha256 = "ac72ad67f7a8c6b18bf605d8602425182b417de4369715bf89146daf62f7ae48",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.1.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.1.1/bazel-8.1.1-darwin-x86_64"],
        sha256 = "3c71b665a86cebf3c63b3d78de1ea22f15c8207dc189a4a40a02959afb911940",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.1.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.1.1/bazel-8.1.1-linux-arm64"],
        sha256 = "359be37055104d84a4a5fa5b570b5bde402a87c2d2b9fc736954bc11366b321e",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.1.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.1.1/bazel-8.1.1-linux-x86_64"],
        sha256 = "a2a095d7006ea70bdfdbe90a71f99f957fee1212d4614cfcfdbe3aadae681def",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.2.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.2.1/bazel-8.2.1-darwin-arm64"],
        sha256 = "22ff65b05869f6160e5157b1b425a14a62085d71d8baef571f462b8fe5a703a3",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.2.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.2.1/bazel-8.2.1-darwin-x86_64"],
        sha256 = "366842d097b7aaf8dba4fff2c3ae333e48b28a9d2b440981183aa4afd8a4bf9f",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.2.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.2.1/bazel-8.2.1-linux-arm64"],
        sha256 = "1f50df607751a56ace7775872ffb5dff4c15c25c11a2df30b7b0035ef53a9937",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.2.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.2.1/bazel-8.2.1-linux-x86_64"],
        sha256 = "7ff2b6a675b59a791d007c526977d5262ade8fa52efc8e0d1ff9e18859909fc0",
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
        sha256 = "43dd6dff759ab7b8abfb5a9d34b4ef1ea97de735141436cc77db1a90359991c3",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-aarch64-v5.10-43dd6dff759ab7b8abfb5a9d34b4ef1ea97de735141436cc77db1a90359991c3"],
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
        urls = ["https://github.com/containerd/containerd/releases/download/v2.1.1/containerd-2.1.1-linux-amd64.tar.gz"],
        sha256 = "918e88fd393c28c89424e6535df0546ca36c1dfa7d8a5d685dee70b449380a9b",
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
        urls = ["https://github.com/containerd/containerd/releases/download/v2.1.1/containerd-2.1.1-linux-arm64.tar.gz"],
        sha256 = "4e3c8c0c2e61438bb393a9ea6bb94f8f56b559ec3243d7b1a2943117bca4dcb4",
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
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.3.0/runc.amd64"],
        sha256 = "028986516ab5646370edce981df2d8e8a8d12188deaf837142a02097000ae2f2",
        downloaded_file_path = "runc",
        executable = True,
    )
    http_file(
        name = "com_github_opencontainers_runc_runc-linux-arm64",
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.3.0/runc.arm64"],
        sha256 = "85c5e4e4f72e442c8c17bac07527cd4f961ee48e4f2b71797f7533c94f4a52b9",
        downloaded_file_path = "runc",
        executable = True,
    )

install_static_dependencies_ext = modules.as_extension(install_static_dependencies)
