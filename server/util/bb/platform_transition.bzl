load("@with_cfg.bzl", "with_cfg")

linux_x86_64_musl_alias, _linux_x86_64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_x86_64_musl")])
        .set("compilation_mode", "opt")
        .build()
)

linux_arm64_musl_alias, _linux_arm64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_arm64_musl")])
        .set("compilation_mode", "opt")
        .build()
)
