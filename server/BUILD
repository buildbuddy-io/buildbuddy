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

genquery(
    name = "server_enterprise_deps",
    expression = "filter(//enterprise, deps(//server))",
    scope = ["//server"],
)

sh_test(
    name = "server_no_enterprise_deps_test",
    srcs = ["server_no_enterprise_deps_test.sh"],
    args = ["$(rootpath :server_enterprise_deps)"],
    data = [":server_enterprise_deps"],
)
