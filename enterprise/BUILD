load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

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
        "//enterprise:config_files",
        "//enterprise:licenses",
        "//:config_files",
        "//static",
    ] + select({
        ":fastbuild": [],
        "//conditions:default": [
            "//enterprise/app:app_bundle",
            "//enterprise/app:style",
            "//enterprise/app:sha",
            "//enterprise/server/cmd/ci_runner:buildbuddy_ci_runner",
        ],
    }) + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
        ],
        "//conditions:default": [],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise",
    deps = [
        "//server/util/fileresolver",
    ],
)
