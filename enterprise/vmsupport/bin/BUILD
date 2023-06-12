package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "mkinitrd",
    srcs = [
        "//enterprise/server/cmd/goinit",
    ],
    outs = ["initrd.cpio"],
    cmd_bash = """
        env \
            GOINIT=$(location //enterprise/server/cmd/goinit:goinit) \
            CPIO=$(location //enterprise/tools/cpio) \
            ./$(location mkinitrd.sh) \
            "$@"
    """,
    tools = [
        "mkinitrd.sh",
        "//enterprise/tools/cpio",
    ],
)

genrule(
    name = "vmlinux_copy",
    srcs = ["@org_kernel_git_linux_kernel-vmlinux//file:downloaded"],
    outs = ["vmlinux"],
    cmd = "cp $(SRCS) $@",
)
