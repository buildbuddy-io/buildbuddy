load("@com_github_sluongng_nogo_analyzer//staticcheck:def.bzl", "ANALYZERS", "staticcheck_analyzers")
load("@bazel_gazelle//:def.bzl", "DEFAULT_LANGUAGES", "gazelle", "gazelle_binary")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "nogo")
load("@npm//@bazel/typescript:index.bzl", "ts_config")
load("//rules/go:index.bzl", "go_sdk_tool")

package(default_visibility = ["//visibility:public"])

nogo(
    name = "vet",
    config = "nogo_config.json",
    vet = True,
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_tools//go/analysis/passes/asmdecl:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/assign:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/atomicalign:go_default_library",
        # "@org_golang_x_tools//go/analysis/passes/cgocall:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/composite:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/copylock:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/deepequalerrors:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/errorsas:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/fieldalignment:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/framepointer:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/httpresponse:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/ifaceassert:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/loopclosure:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/lostcancel:go_default_library",
        # "@org_golang_x_tools//go/analysis/passes/nilness:go_default_library", # template methods currently cause this analyzer to panic
        # "@org_golang_x_tools//go/analysis/passes/shadow:go_default_library", # Everyone shadows `err`
        "@org_golang_x_tools//go/analysis/passes/shift:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/sortslice:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/stdmethods:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/stringintconv:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/structtag:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/tests:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/testinggoroutine:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/unmarshal:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/unreachable:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/unsafeptr:go_default_library",
        "@org_golang_x_tools//go/analysis/passes/unusedresult:go_default_library",
        "@com_github_nishanths_exhaustive//:exhaustive",
    ] + staticcheck_analyzers(ANALYZERS + [
        "-S1002",
        "-S1004",
        "-S1005",
        "-S1007",
        "-S1008",
        "-S1009",
        "-S1011",
        "-S1012",
        "-S1017",
        "-S1019",
        "-S1023",
        "-S1025",
        "-S1028",
        "-S1030",
        "-S1031",
        "-S1032",
        "-S1034",
        "-S1037",
        "-S1024",
        "-SA1012",
        "-SA1019",
        "-SA1024",
        "-SA1029",
        "-SA4006",
        "-SA4009",
        "-SA4010",
        "-SA4011",
        "-SA4021",
        "-SA5001",
        "-SA5011",
        "-SA6002",
        "-SA9001",
        "-ST1000",
        "-ST1003",
        "-ST1005",
        "-ST1006",
        "-ST1008",
        "-ST1012",
        "-ST1016",
        "-ST1017",
        "-ST1019",
        "-ST1020",
        "-ST1021",
        "-ST1022",
        "-ST1023",
        "-QF1001",
        "-QF1003",
        "-QF1004",
        "-QF1005",
        "-QF1006",
        "-QF1008",
        "-QF1011",
        "-QF1012",
    ]),
)

gazelle_binary(
    name = "bb_gazelle_binary",
    languages = DEFAULT_LANGUAGES + ["@bazel_gazelle//language/bazel/visibility:go_default_library"],
)

# Ignore the node_modules dir
# gazelle:exclude node_modules
# Ignore generated proto files
# gazelle:exclude **/*.pb.go
# gazelle:exclude bundle.go
# gazelle:exclude enterprise/bundle.go
# Prefer generated BUILD files to be called BUILD over BUILD.bazel
# gazelle:build_file_name BUILD,BUILD.bazel
# gazelle:prefix github.com/buildbuddy-io/buildbuddy
# gazelle:proto disable
# gazelle:map_kind ts_project ts_library //rules/typescript:index.bzl
# gazelle:exclude **/node_modules/**
# TODO(siggisim): remove once we support .css imports properly
# gazelle:exclude website/**
gazelle(
    name = "gazelle",
    gazelle = ":bb_gazelle_binary",
)

# Example usage: "bazel run //:gofmt -- -w ."
go_sdk_tool(
    name = "gofmt",
    goroot_relative_path = "bin/gofmt",
)

exports_files([
    ".swcrc",
    "package.json",
    "yarn.lock",
])

ts_config(
    name = "tsconfig",
    src = ":tsconfig.json",
)

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

config_setting(
    name = "fastbuild",
    values = {"compilation_mode": "fastbuild"},
)

# Synthesize a copy of the file in the current package so it can be embedded.
genrule(
    name = "aws_rds_certs",
    srcs = ["@aws_rds_certs//file:rds-combined-ca-bundle.pem"],
    outs = ["rds-combined-ca-bundle.pem"],
    cmd_bash = "cp $(SRCS) $@",
)

# Certs that are distributed with the server binary.
filegroup(
    name = "embedded_certs",
    srcs = [":rds-combined-ca-bundle.pem"],
)

# N.B. this is ignored by gazelle so must be updated by hand.
# It must live at the repo root to be able to bundle other files using
# "go:embed".
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [
        "//:config_files",
        "//:embedded_certs",
        "//static",
    ] + select({
        ":fastbuild": [],
        "//conditions:default": [
            "//app:app_bundle",
            "//app:style.css",
            "//app:sha",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy",
    deps = [
        "//server/util/fileresolver",
    ],
)

platform(
    name = "firecracker",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
    exec_properties = {
        "workload-isolation-type": "firecracker",
    },
)

platform(
    name = "firecracker_vfs",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
    exec_properties = {
        "workload-isolation-type": "firecracker",
        "enable-vfs": "true",
    },
)

platform(
    name = "vfs",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
    exec_properties = {
        "enable-vfs": "true",
    },
)

# TODO(bduffany): The sh_toolchain config here is a workaround for
# https://github.com/aspect-build/rules_swc/issues/20
# We should probably either move these to the buildbuddy-toolchain repo
# or add a symlink from /usr/bin/bash -> /bin/bash to remove the need for these.
load("@bazel_tools//tools/sh:sh_toolchain.bzl", "sh_toolchain")

sh_toolchain(
    name = "bash_rbe_ubuntu1604",
    path = "/bin/bash",
)

toolchain(
    name = "sh_toolchain",
    toolchain = ":bash_rbe_ubuntu1604",
    toolchain_type = "@bazel_tools//tools/sh:toolchain_type",
)
