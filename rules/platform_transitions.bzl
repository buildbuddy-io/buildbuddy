load("@with_cfg.bzl", "with_cfg")

linux_x86_64_alias, _linux_x86_64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:linux_x86_64")])
        .build()
)

linux_arm64_alias, _linux_arm64_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("@toolchains_buildbuddy//platforms:linux_arm64")])
        .build()
)

linux_x86_64_musl_alias, _linux_x86_64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_x86_64_musl")])
        .extend(Label("@io_bazel_rules_go//go/config:tags"), ["static"])
        .build()
)

linux_arm64_musl_alias, _linux_arm64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_arm64_musl")])
        .extend(Label("@io_bazel_rules_go//go/config:tags"), ["static"])
        .build()
)
