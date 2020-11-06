package(
    default_visibility = ["//visibility:public"],
)

[alias(
    name = t,
    actual = "//server/cmd/buildbuddy:%s" % t,
) for t in [
    "buildbuddy",  # go_binary
    "base_image",  # container_image
    "buildbuddy_image",  # go_image
]]

alias(
    name = "server",
    actual = ":buildbuddy",
)
