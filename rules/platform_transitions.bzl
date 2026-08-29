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
        .build()
)

linux_arm64_musl_alias, _linux_arm64_musl_alias = (
    with_cfg(native.alias)
        .set("platforms", [Label("//platforms:linux_arm64_musl")])
        .build()
)

# Linux-only select for go_binary's `static` attribute: force fully static
# linking on Linux (needed under the musl transition, where cgo is enabled and
# the Go linker would otherwise emit a binary that depends on musl's ld.so),
# "auto" elsewhere (macOS cannot link statically).
STATIC_ON_LINUX = select({
    "//platforms/configs:linux_x86_64": "on",
    "//platforms/configs:linux_arm64": "on",
    "//conditions:default": "auto",
})

def embedded_static_binary(name, actual, visibility = None):
    """Exposes `actual` as a statically linked Linux binary for embedding.

    Creates `<name>` as an alias that resolves, by target CPU, to `actual`
    built through the musl platform transition (linux_x86_64_musl_alias /
    linux_arm64_musl_alias). All binaries embedded into the executor (the bb
    CLI, goinit, ci_runner) go through this one transition so they share a
    single Bazel configuration -- and thus one Go stdlib and one compile of
    every shared package -- in cold builds.

    `actual` must set `static = STATIC_ON_LINUX` so the result is fully static.

    Args:
        name: name of the alias to create.
        actual: label of the go_binary to build.
        visibility: visibility of the alias.
    """
    linux_x86_64_musl_alias(
        name = name + "_linux_x86_64",
        actual = actual,
        tags = ["manual"],
        visibility = visibility,
    )
    linux_arm64_musl_alias(
        name = name + "_linux_arm64",
        actual = actual,
        tags = ["manual"],
        visibility = visibility,
    )
    # manual: the select has no default branch on purpose (Linux-only), and
    # tags do not propagate through aliases, so without this a wildcard build
    # on macOS would fail on these targets.
    native.alias(
        name = name,
        actual = select({
            "//platforms/configs:linux_x86_64": ":" + name + "_linux_x86_64",
            "//platforms/configs:linux_arm64": ":" + name + "_linux_arm64",
        }),
        tags = ["manual"],
        visibility = visibility,
    )
