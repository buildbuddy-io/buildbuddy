package(default_visibility = ["//visibility:public"])

# The macOS downloads replace themselves with the decompressed executable on
# first run. Prepare a copy at build time so concurrent replicas can share an
# immutable runfile, without modifying the downloaded artifact.
[
    genrule(
        name = "clickhouse_darwin_" + arch,
        srcs = ["@clickhouse_darwin_" + arch + "//file"],
        outs = ["darwin_" + arch + "/clickhouse"],
        cmd = """
            cp "$(SRCS)" "$@"
            chmod +x "$@"
            # Running --version makes the downloaded executable decompress itself.
            "$@" --version
        """,
        exec_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:" + cpu,
        ],
        executable = True,
        target_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:" + cpu,
        ],
    )
    for arch, cpu in [
        ("amd64", "x86_64"),
        ("arm64", "arm64"),
    ]
]

filegroup(
    name = "clickhouse_bin",
    srcs = select({
        "@io_bazel_rules_go//go/platform:darwin_amd64": [":clickhouse_darwin_amd64"],
        "@io_bazel_rules_go//go/platform:darwin_arm64": [":clickhouse_darwin_arm64"],
        "@io_bazel_rules_go//go/platform:linux_amd64": ["@clickhouse_linux_amd64//:usr/bin/clickhouse"],
        "@io_bazel_rules_go//go/platform:linux_arm64": ["@clickhouse_linux_arm64//:usr/bin/clickhouse"],
        "//conditions:default": [],
    }),
    target_compatible_with = select({
        "@io_bazel_rules_go//go/platform:darwin_amd64": [],
        "@io_bazel_rules_go//go/platform:darwin_arm64": [],
        "@io_bazel_rules_go//go/platform:linux_amd64": [],
        "@io_bazel_rules_go//go/platform:linux_arm64": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)
