load("@bazel_gazelle//:def.bzl", "gazelle")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "nogo")

package(default_visibility = ["//visibility:public"])

nogo(
    name = "vet",
    config = "nogo_config.json",
    vet = True,
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_tools//go/analysis/passes/asmdecl:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/assign:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/atomic:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/atomicalign:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/bools:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/buildtag:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/cgocall:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/composite:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/copylock:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/deepequalerrors:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/errorsas:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/fieldalignment:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/framepointer:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/httpresponse:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/ifaceassert:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/loopclosure:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/lostcancel:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/nilfunc:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/nilness:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/printf:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/shadow:go_tool_library", # Everyone shadows `err`
        "@org_golang_x_tools//go/analysis/passes/shift:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/sortslice:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/stdmethods:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/stringintconv:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/structtag:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/tests:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/testinggoroutine:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/unmarshal:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/unreachable:go_tool_library",
        # "@org_golang_x_tools//go/analysis/passes/unsafeptr:go_tool_library",
        "@org_golang_x_tools//go/analysis/passes/unusedresult:go_tool_library",
        "@com_github_nishanths_exhaustive//:exhaustive",
    ],
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
    embedsrcs = select({
        ":fastbuild": [
            "//:VERSION",
            "//:config_files",
            "//static",
        ],
        "//conditions:default": [
            "//:VERSION",
            "//:config_files",
            "//app:app_bundle",
            "//app:style.css",
            "//static",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy",
    deps = [
        "//server/util/log",
    ],
)
