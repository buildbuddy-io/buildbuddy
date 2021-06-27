exports_files(["vmlinux"])

genrule(
    name = "mkinitrd",
    srcs = [
        "//enterprise/server/cmd/goinit",
    ],
    outs = ["initrd.cpio"],
    cmd = "./$(location mkinitrd.sh) $(location //enterprise/server/cmd/goinit:goinit) \"$@\"",
    tags = ["manual"],
    tools = ["mkinitrd.sh"],
    visibility = ["//visibility:public"],
)
