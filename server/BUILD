package(
    default_visibility = ["//visibility:public"],
)

alias(
    name = "buildbuddy",
    actual = "//server/cmd/buildbuddy:buildbuddy",  # go_binary
)

[alias(
    name = t,
    actual = "//server/cmd/buildbuddy:%s" % t,
    tags = ["manual"],
) for t in [
    "base_image",  # container_image
    "buildbuddy_image",  # container_image
]]

alias(
    name = "server",
    actual = ":buildbuddy",
)
