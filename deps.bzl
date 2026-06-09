load("@bazel_skylib//lib:modules.bzl", "modules")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# The podman version used in tests and distributed with the buildbuddy executor image.
# When changing this version, a new release of podman-static may be needed.
# See dockerfiles/executor_image/README.md for instructions.
# The checksums below will also need to be updated.
PODMAN_VERSION = "v5.7.0"
PODMAN_STATIC_SHA256_AMD64 = "6a1c06b78d7dad15d8d7155a180874939a04bd39ce2f64726c7f11142ab7aa7d"
PODMAN_STATIC_SHA256_ARM64 = "703ffad8972aa2db70a173c80804a88185e6c2dc8a88a247a8ebffeac424b0ba"

# The bb CLI version used by //tools/lint for `bb fix` and `bb mod deps`.
BB_CLI_VERSION = "5.0.333"

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
            'filegroup(name = "firecracker", srcs = ["release-{release}/firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["release-{release}/jailer-{release}"])',
        ]).format(release = "v1.15.1-x86_64"),
        sha256 = "d4a32ab2322d887ca1bc4a4e7afa9cc35393e6362dfc2b3becb389d362e4275a",
        urls = ["https://github.com/firecracker-microvm/firecracker/releases/download/v1.15.1/firecracker-v1.15.1-x86_64.tgz"],
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
        name = "io_bazel_bazel-6.6.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.6.0/bazel-6.6.0-darwin-arm64"],
        sha256 = "e7983caca13ada740ac3714cee07ca4f8add34392c022434ceb94e30844e36d6",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.6.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.6.0/bazel-6.6.0-darwin-x86_64"],
        sha256 = "9503571744f0e2ec455d943b7b248f9ca7eb1adca9edf5c83f876714c7e47ec8",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.6.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.6.0/bazel-6.6.0-linux-arm64"],
        sha256 = "722ecdd63e6f087ad8f3cc6054eebc81b18427824195c05a6b948f4c1589dc78",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-6.6.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.6.0/bazel-6.6.0-linux-x86_64"],
        sha256 = "31a97fd23503f8a3dc50b6c9232cc513fb7cdd1c4a80d4d598673c60ff77b6f3",
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
        name = "io_bazel_bazel-8.5.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.1/bazel-8.5.1-darwin-arm64"],
        sha256 = "cb6d2f19ad92157e7186f64151e665c1b0c3bacaa690784e66f446f1b7660140",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.1/bazel-8.5.1-darwin-x86_64"],
        sha256 = "325b7b8ea32a18b4d62180e4bb15d8d964445f625a11d783f760695c89c2e910",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.1/bazel-8.5.1-linux-arm64"],
        sha256 = "b7f2a85595e8a87d54843bc656c1e379fd9b8c1b5f783dc41717c1d7eb7cc49f",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.5.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.5.1/bazel-8.5.1-linux-x86_64"],
        sha256 = "61d89402f0368e64b6c827be5de79d8e65382e8124c3cbb97325611a1851392e",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.6.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.6.0/bazel-8.6.0-darwin-arm64"],
        sha256 = "948a7186641f601c83344b63b88bc6943025585f2bb7f407e19cface5fe4aa3b",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.6.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.6.0/bazel-8.6.0-darwin-x86_64"],
        sha256 = "27ecdc90ea1e34b8a96950128130e398f3a3e18b16608d1fc9f4101a80eb2c46",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.6.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.6.0/bazel-8.6.0-linux-arm64"],
        sha256 = "afc9f702112df02edb44344c0671b3ad3afa34a9841bd2cc486b61a1ff605e30",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.6.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.6.0/bazel-8.6.0-linux-x86_64"],
        sha256 = "9860da9c9386bbc023feed8f43af3105d338727d77b644fa6aeca45a4a11957c",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.7.0-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.7.0/bazel-8.7.0-darwin-arm64"],
        sha256 = "575f20fb23955e02f73519befd180df635b4ed0960c60f0e70fcc8d74014a713",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.7.0-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.7.0/bazel-8.7.0-darwin-x86_64"],
        sha256 = "76f3eb05782098e9f9ddd8247ec969b085195a3ae2978c81721a2235052ccf26",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.7.0-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.7.0/bazel-8.7.0-linux-arm64"],
        sha256 = "bfe9558bd8a2ecfe4841ec46c0dbccb4b469fe22d81f2f859de0de222b3e7ce3",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-8.7.0-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/8.7.0/bazel-8.7.0-linux-x86_64"],
        sha256 = "d7606e679b78067c811096fb3d6cf135225b528835ca396e3a4dddf957859544",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.2-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.2/bazel-9.0.2-darwin-arm64"],
        sha256 = "525cfcbf9790af7319ea78c9ff2053b8a5634013c41783440b5d82433f14d280",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.2-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.2/bazel-9.0.2-darwin-x86_64"],
        sha256 = "982cfb5ef4fac51aca3520ce03aca19eed5492090b410d1e35cb81c4fd8a7ce8",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.2-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.2/bazel-9.0.2-linux-arm64"],
        sha256 = "e9fc2c7b0027619058acae81c063e5ced683cb0f1598e9373b4f752820c03adb",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.0.2-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.0.2/bazel-9.0.2-linux-x86_64"],
        sha256 = "422e7a1690b76d7e615c29091d3aca28d0bd3a93fe3c93cbefb8f72d774926d5",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.1.1-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.1.1/bazel-9.1.1-darwin-arm64"],
        sha256 = "2db883718453f0437a7bcb408e889dbf8539cdc4d61c8ebc3807a1a88d02ff08",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.1.1-darwin-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.1.1/bazel-9.1.1-darwin-x86_64"],
        sha256 = "6fd490084bdccf044d7a6d8360a26f8770fa09f4e624328efea292f493204930",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.1.1-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.1.1/bazel-9.1.1-linux-arm64"],
        sha256 = "82d1163884e45a6a7ff764cc01197b1b1ed497000726b84dc4b47c1dfc8a2bb4",
        executable = True,
    )
    http_file(
        name = "io_bazel_bazel-9.1.1-linux-x86_64",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/9.1.1/bazel-9.1.1-linux-x86_64"],
        sha256 = "857bed5d2756b4d998d3caebf2d941d13d434c4eda4b1d6d7dda205736c25a93",
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
        sha256 = "0e1ec2bd6a3a6e5a50b220401dd14174eee234b532dcf1279777a181221d502f",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-x86_64-v5.15-0e1ec2bd6a3a6e5a50b220401dd14174eee234b532dcf1279777a181221d502f"],
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux-6.1",
        sha256 = "10ec6f850aff3fe98fdaa603b299fb22ad67d5013b7a28801a7c6b75e4c3406f",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-x86_64-v6.1-10ec6f850aff3fe98fdaa603b299fb22ad67d5013b7a28801a7c6b75e4c3406f"],
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux-arm64",
        sha256 = "e6870fdc288621a5c9e3424bcd7ddee03816483e61846368de25e93f3db8e52e",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-aarch64-v5.10-e6870fdc288621a5c9e3424bcd7ddee03816483e61846368de25e93f3db8e52e"],
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
        name = "io_buildbuddy_bb_cli-darwin-arm64",
        executable = True,
        integrity = "sha256-NIFvkFPQj2UW9voLTzgbVup7WAC6vzJo3w316d1sKVI=",
        urls = ["https://github.com/buildbuddy-io/bazel/releases/download/{version}/bazel-{version}-darwin-arm64".format(version = BB_CLI_VERSION)],
    )

    http_file(
        name = "io_buildbuddy_bb_cli-darwin-x86_64",
        executable = True,
        integrity = "sha256-B0rhObAwP/aGdtwnw7iZuCarOTS/jxp2eaUE8M67o5A=",
        urls = ["https://github.com/buildbuddy-io/bazel/releases/download/{version}/bazel-{version}-darwin-x86_64".format(version = BB_CLI_VERSION)],
    )

    http_file(
        name = "io_buildbuddy_bb_cli-linux-arm64",
        executable = True,
        integrity = "sha256-08m6+utxtg0CrYDgeNz/ZkZprSLeTMkjSKe5BPD9SW8=",
        urls = ["https://github.com/buildbuddy-io/bazel/releases/download/{version}/bazel-{version}-linux-arm64".format(version = BB_CLI_VERSION)],
    )

    http_file(
        name = "io_buildbuddy_bb_cli-linux-x86_64",
        executable = True,
        integrity = "sha256-TaoceuYO8XWwHFvEYAoEyBb2CvQsLnLIzVft9qwr5f0=",
        urls = ["https://github.com/buildbuddy-io/bazel/releases/download/{version}/bazel-{version}-linux-x86_64".format(version = BB_CLI_VERSION)],
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
