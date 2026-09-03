"""alias() transition wrappers for platforms we target."""

load("@with_cfg.bzl", "with_cfg")

linux_x86_64_alias, _linux_x86_64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:linux_x86_64")])
        .build()
)
linux_x86_64_musl_alias, _linux_x86_64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_x86_64_musl")])
        .build()
)

linux_arm64_alias, _linux_arm64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:linux_arm64")])
        .build()
)
linux_arm64_musl_alias, _linux_arm64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_arm64_musl")])
        .build()
)

freebsd_x86_64_alias, _freebsd_x86_64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:freebsd_x86_64")])
        .build()
)

openbsd_x86_64_alias, _openbsd_x86_64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:openbsd_x86_64")])
        .build()
)

macos_arm64_alias, _macos_arm64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:darwin_arm64")])
        .build()
)

macos_x86_64_alias, _macos_x86_64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:darwin_x86_64")])
        .build()
)

windows_amd64_alias, _windows_amd64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@io_bazel_rules_go//go/toolchain:windows_amd64")])
        .build()
)

windows_arm64_alias, _windows_arm64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@io_bazel_rules_go//go/toolchain:windows_arm64")])
        .build()
)

windows_386_alias, _windows_386_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@io_bazel_rules_go//go/toolchain:windows_386")])
        .build()
)
