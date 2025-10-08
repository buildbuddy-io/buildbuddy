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
    srcs = ["@kernel_bins//tools/org_kernel_git_linux_kernel-vmlinux"],
    outs = ["vmlinux"],
    cmd = "cp $(SRCS) $@",
)

genrule(
    name = "vmlinux_6.1_copy",
    srcs = ["@kernel_bins//tools/org_kernel_git_linux_kernel-vmlinux-6_1"],
    outs = ["vmlinux_6.1"],
    cmd = "cp $(SRCS) $@",
    target_compatible_with = ["@platforms//cpu:x86_64"],
)
