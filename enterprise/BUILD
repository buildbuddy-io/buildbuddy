package(default_visibility = [":__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

filegroup(
    name = "config_files",
    srcs = select({
        ":release_build": [
            "config/buildbuddy.release.yaml",
            "config/executor.release.yaml",
        ],
        "//conditions:default": glob(["config/**"]),
    }),
)

config_setting(
    name = "release_build",
    values = {"define": "release=true"},
)

filegroup(
    name = "licenses",
    srcs = [
        "LICENSE",
    ],
    visibility = ["//visibility:public"],
)

config_setting(
    name = "fastbuild",
    values = {"compilation_mode": "fastbuild"},
)

# N.B. this is ignored by gazelle so must be updated by hand.
# It must live at the repo root to be able to bundle other files using
# "go:embed".
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [
        "//:VERSION",
        "//enterprise:config_files",
        "//enterprise:licenses",
        "//:config_files",
        "//static",
        "//enterprise/vmsupport/bin:vmlinux",
        "//enterprise/vmsupport/bin:initfs",
    ] + select({
        ":fastbuild": [],
        "//conditions:default": [
            "//enterprise/app:app_bundle",
            "//enterprise/app:style",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//server/util/log",
    ],
)
