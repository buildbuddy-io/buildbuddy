package(default_visibility = ["//visibility:public"])

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
platform(
    name = "linux_x86_64_musl",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        "//toolchains:musl_on",
    ],
)

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
platform(
    name = "linux_arm64_musl",
    constraint_values = [
        "@platforms//cpu:arm64",
        "@platforms//os:linux",
        "//toolchains:musl_on",
    ],
)

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
)

platform(
    name = "linux_arm64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:arm64",
    ],
)

# Execution platform for building via a local `actiond` hermetic VM worker
# (github.com/hermeticbuild/actiond). Inherits the standard BuildBuddy linux
# x86_64 toolchain platform (CC toolchain resolution, OSFamily, container-image)
# and adds the exec_properties that actiond reads to provision its empty guest
# chroot: `requires-bash` mounts the embedded bash runtime, and `libc` mounts a
# glibc so prebuilt exec tools (protoc, clang, etc.) can run. Use as
# --extra_execution_platforms while keeping --platforms on a normal target
# platform. See //:user.bazelrc `--config=actiond` (or actiond.bazelrc).
# Execution platform for building inside a local actiond hermetic VM worker.
# Deliberately a plain linux/x86_64 platform (NO @bazel_tools//tools/cpp:gcc
# constraint): with the musl target platform this lets the hermetic
# toolchains_musl CC toolchain win resolution, exactly as the default host exec
# platform does. Adding the gcc constraint instead makes the autodetected
# /usr/bin/gcc toolchain match and win — which then fails in actiond's empty
# chroot because the host gcc is not an action input. The exec_properties are
# what actiond reads to provision the guest chroot: `requires-bash` mounts the
# embedded bash + busybox/coreutils runtime; `libc` mounts a glibc so any
# glibc-linked exec tool can run.
platform(
    name = "actiond_linux_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        # musl_on so EXEC-config (tool) actions also resolve the hermetic
        # toolchains_musl CC toolchain. Without it, exec-config cgo (e.g. the Go
        # stdlib's runtime/cgo built for tools) invokes the non-hermetic `cc`,
        # which does not exist in actiond's empty chroot.
        "//toolchains:musl_on",
    ],
    exec_properties = {
        "OSFamily": "Linux",
        "Arch": "amd64",
        "requires-bash": "",
        "libc": "glibc2.39",
    },
)
