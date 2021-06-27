genrule(
    name = "mkinitfs",
    srcs = [
        "//enterprise/server/cmd/goinit",
    ],
    outs = ["initfs"],
    cmd = "./$(location mkinitfs.sh) $(location //enterprise/server/cmd/goinit:goinit) \"$@\"",
    tags = ["manual"],
    tools = ["mkinitfs.sh"],
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
