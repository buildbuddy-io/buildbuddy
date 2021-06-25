genrule(
    name = "mkinitfs",
    srcs = [
        "//enterprise/server/cmd/goinit",
    ],
    outs = ["initfs"],
    cmd = "./$(location mkinitfs.sh) $(location //enterprise/server/cmd/goinit:goinit) \"$@\"",
    tools = ["mkinitfs.sh"],
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
