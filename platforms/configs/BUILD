package(default_visibility = ["//visibility:public"])

config_setting(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
)

config_setting(
    name = "linux_arm64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:arm64",
    ],
)

config_setting(
    name = "macos_x86_64",
    constraint_values = [
        "@platforms//os:macos",
        "@platforms//cpu:x86_64",
    ],
)

config_setting(
    name = "macos_arm64",
    constraint_values = [
        "@platforms//os:macos",
        "@platforms//cpu:arm64",
    ],
)

# Matches Linux builds that set the "static" Go build tag, which the musl
# platforms in //platforms apply. Use this in a select() alongside
# @io_bazel_rules_go//go/platform:linux to drop deps that cannot be built or
# linked statically. Bazel picks this setting over the plain linux one when
# both match, because it is a strict specialization of it.
config_setting(
    name = "linux_static",
    constraint_values = ["@platforms//os:linux"],
    flag_values = {"@io_bazel_rules_go//go/config:tags": "static"},
)
