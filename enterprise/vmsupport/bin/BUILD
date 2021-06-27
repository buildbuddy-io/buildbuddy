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

genrule(
    name = "mkinitrd",
    srcs = [
        "//enterprise/server/cmd/goinit",
    ],
    outs = ["initrd"],
    cmd = "./$(location mkinitrd.sh) $(location //enterprise/server/cmd/goinit:goinit) \"$@\"",
    tags = ["manual"],
    tools = ["mkinitrd.sh"],
    visibility = ["//visibility:public"],
)
