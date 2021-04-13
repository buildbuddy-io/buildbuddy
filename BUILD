load("@bazel_gazelle//:def.bzl", "gazelle")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "nogo")

package(default_visibility = ["//visibility:public"])

nogo(
    name = "vet",
    vet = True,
    visibility = ["//visibility:public"],
)

# Ignore the node_modules dir
# gazelle:exclude node_modules
# Ignore generated proto files
# gazelle:exclude **/*.pb.go
# gazelle:exclude bundle.go
# Prefer generated BUILD files to be called BUILD over BUILD.bazel
# gazelle:build_file_name BUILD,BUILD.bazel
# gazelle:prefix github.com/buildbuddy-io/buildbuddy
# gazelle:proto disable
gazelle(name = "gazelle")

exports_files([
    "tsconfig.json",
    "package.json",
    "yarn.lock",
    "VERSION",
])

filegroup(
    name = "config_files",
    srcs = select({
        ":release_build": ["config/buildbuddy.release.yaml"],
        "//conditions:default": glob(["config/**"]),
    }),
)

config_setting(
    name = "release_build",
    values = {"define": "release=true"},
)

package_group(
    name = "os",
    packages = [
        "//app/...",
        "//config/...",
        "//deployment/...",
        "//docs/...",
        "//node_modules/...",
        "//proto/...",
        "//rules/...",
        "//server/...",
        "//static/...",
        "//templates/...",
        "//tools/...",
    ],
)

package_group(
    name = "enterprise",
    packages = [
        "//enterprise/...",
    ],
)

# N.B. this is ignored by gazelle so must be updated by hand.
# It must live at the repo root to be able to bundle other files using
# "go:embed".
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [
        "//:VERSION",
        "//:config_files",
        "//app:app_bundle",
        "//app:style.css",
        "//static",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/bundle",
    deps = [
        "//server/util/log",
    ],
)
