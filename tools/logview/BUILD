load("@npm//@bazel/esbuild:index.bzl", "esbuild")
load("//rules/typescript:index.bzl", "ts_library")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "logview_lib",
    srcs = ["logview.go"],
    embedsrcs = [
        "style.css",
        "index.html",
        "main_bundle.js",
        "prompt.md",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/logview",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

go_binary(
    name = "logview",
    embed = [":logview_lib"],
    visibility = ["//visibility:public"],
)

# CSS

genrule(
    name = "style",
    srcs = [
        "monaco.css",
        "index.css",
    ],
    outs = ["style.css"],
    cmd_bash = "cat $(SRCS) > $@",
)

# JS

esbuild(
    name = "bundle",
    args = {
        "resolveExtensions": [
            ".mjs",
            ".js",
        ],
        "loader": {
            ".ttf": "binary",
            ".css": "binary",
        },
    },
    define = {"process.env.NODE_ENV": '"production"'},
    entry_points = ["main.tsx"],
    metafile = False,
    minify = select({
        "//:fastbuild": False,
        "//conditions:default": True,
    }),
    deps = [
        ":main",
    ],
)

genrule(
    name = "main_bundle",
    srcs = [":bundle"],
    outs = ["main_bundle.js"],
    cmd_bash = "cp $(location :bundle)/main.js $@",
)

ts_library(
    name = "main",
    srcs = ["main.tsx"],
    deps = [
        "//tools/logview:App",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//react",
        "@npm//react-dom",
    ],
)

ts_library(
    name = "logs",
    srcs = ["logs.tsx"],
)

ts_library(
    name = "filter",
    srcs = ["filter.tsx"],
    deps = [
        "//tools/logview:logs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "colors",
    srcs = ["colors.tsx"],
)

ts_library(
    name = "severity",
    srcs = ["severity.tsx"],
)

ts_library(
    name = "query",
    srcs = ["query.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "App",
    srcs = ["App.tsx"],
    deps = [
        "//tools/logview:QueryEditor",
        "//tools/logview:colors",
        "//tools/logview:filter",
        "//tools/logview:logs",
        "//tools/logview:severity",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "QueryEditor",
    srcs = ["QueryEditor.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//tslib",
    ],
)
