package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "vmlinux",
])

genrule(
    name = "mkinitrd",
    srcs = [
        "//enterprise/server/cmd/goinit",
        "//enterprise/server/vmvfs",
    ],
    outs = ["initrd.cpio"],
    cmd_bash = """
        env \
            GOINIT=$(location //enterprise/server/cmd/goinit:goinit) \
            VMVFS=$(location //enterprise/server/vmvfs) \
            CPIO=$(location //enterprise/tools/cpio) \
            ./$(location mkinitrd.sh) \
            "$@"
    """,
    tools = [
        "mkinitrd.sh",
        "//enterprise/tools/cpio",
    ],
)
