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
    srcs = select({
        "@platforms//cpu:arm64": ["@org_kernel_git_linux_kernel-vmlinux-arm64//file:downloaded"],
        "//conditions:default": ["@org_kernel_git_linux_kernel-vmlinux//file:downloaded"],
    }),
    outs = ["vmlinux"],
    cmd = "cp $(SRCS) $@",
)

genrule(
    name = "vmlinux_6.1_copy",
    srcs = ["@org_kernel_git_linux_kernel-vmlinux-6.1//file:downloaded"],
    outs = ["vmlinux_6.1"],
    cmd = "cp $(SRCS) $@",
    target_compatible_with = ["@platforms//cpu:x86_64"],
)
