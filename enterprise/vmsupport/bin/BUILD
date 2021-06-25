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
