load("@bazel_gazelle//:def.bzl", "DEFAULT_LANGUAGES", "gazelle", "gazelle_binary")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@com_github_sluongng_nogo_analyzer//staticcheck:def.bzl", "ANALYZERS", "staticcheck_analyzers")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "nogo")
load("@npm//@bazel/typescript:index.bzl", "ts_config")
load("//rules/go:index.bzl", "go_sdk_tool")

package(default_visibility = ["//visibility:public"])

# Rendered JSON result could be checked by doing:
#   bazel build //:no_go_config
#   cat bazel-bin/no_go_config.json | jq .
write_file(
    name = "nogo_config",
    out = "nogo_config.json",
    content = [
        json.encode_indent(
            {
                "exhaustive": {
                    "exclude_files": {
                        "external[\\\\,\\/]": "third_party",
                    },
                    "analyzer_flags": {
                        "default-signifies-exhaustive": "true",
                    },
                },
            } | {
                analyzer: {
                    "exclude_files": {
                        "external[\\\\,\\/]": "third_party",
                        # TODO(sluongng): this should be fixed on rules_go side
                        # https://github.com/bazelbuild/rules_go/issues/3619
                        "cgo[\\\\,\\/]github.com[\\\\,\\/]shirou[\\\\,\\/]gopsutil[\\\\,\\/]": "third_party cgo",
                    },
                }
                for analyzer in ANALYZERS + [
                    "asmdecl",
                    "assign",
                    "atomicalign",
                    # "cgocall",
                    "buildtag",
                    "cgocall",
                    "composites",
                    "copylocks",
                    "deepequalerrors",
                    "errorsas",
                    # "fieldalignment",
                    "framepointer",
                    "httpresponse",
                    "ifaceassert",
                    "loopclosure",
                    "lostcancel",
                    # template methods currently cause this analyzer to panic
                    # "nilness",
                    # Everyone shadows `err`
                    # "shadow",
                    "shift",
                    "sortslice",
                    "stdmethods",
                    "stringintconv",
                    "structtag",
                    "tests",
                    "testinggoroutine",
                    "unmarshal",
                    "unreachable",
                    "unsafeptr",
                    "unusedresult",
                ]
            },
        ),
    ],
)

nogo(
    name = "vet",
    config = ":nogo_config.json",
    vet = True,
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_tools//go/analysis/passes/asmdecl",
        "@org_golang_x_tools//go/analysis/passes/assign",
        "@org_golang_x_tools//go/analysis/passes/atomicalign",
        # "@org_golang_x_tools//go/analysis/passes/cgocall",
        "@org_golang_x_tools//go/analysis/passes/composite",
        "@org_golang_x_tools//go/analysis/passes/copylock",
        "@org_golang_x_tools//go/analysis/passes/deepequalerrors",
        "@org_golang_x_tools//go/analysis/passes/errorsas",
        # "@org_golang_x_tools//go/analysis/passes/fieldalignment",
        "@org_golang_x_tools//go/analysis/passes/framepointer",
        "@org_golang_x_tools//go/analysis/passes/httpresponse",
        "@org_golang_x_tools//go/analysis/passes/ifaceassert",
        "@org_golang_x_tools//go/analysis/passes/loopclosure",
        "@org_golang_x_tools//go/analysis/passes/lostcancel",
        # template methods currently cause this analyzer to panic
        # "@org_golang_x_tools//go/analysis/passes/nilness",
        # Everyone shadows `err`
        # "@org_golang_x_tools//go/analysis/passes/shadow",
        "@org_golang_x_tools//go/analysis/passes/shift",
        "@org_golang_x_tools//go/analysis/passes/sortslice",
        "@org_golang_x_tools//go/analysis/passes/stdmethods",
        "@org_golang_x_tools//go/analysis/passes/stringintconv",
        "@org_golang_x_tools//go/analysis/passes/structtag",
        "@org_golang_x_tools//go/analysis/passes/tests",
        "@org_golang_x_tools//go/analysis/passes/testinggoroutine",
        "@org_golang_x_tools//go/analysis/passes/unmarshal",
        "@org_golang_x_tools//go/analysis/passes/unreachable",
        "@org_golang_x_tools//go/analysis/passes/unsafeptr",
        "@org_golang_x_tools//go/analysis/passes/unusedresult",
        "@com_github_nishanths_exhaustive//:exhaustive",
    ] + staticcheck_analyzers(ANALYZERS + [
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

go_sdk_tool(
    name = "go",
    goroot_relative_path = "bin/go",
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
