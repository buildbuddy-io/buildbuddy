load("@aspect_rules_esbuild//esbuild:defs.bzl", "esbuild")
load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "index_js",
    srcs = ["//enterprise/tools/databuddy/app:app_bundle"],
    outs = ["index.js"],
    cmd_bash = "cp $(SRCS)/index.js $@",
)

genrule(
    name = "monaco_css",
    srcs = ["//:node_modules/monaco-editor/dir"],
    outs = ["monaco_editor.main.css"],
    cmd_bash = "cp $(SRCS)/min/vs/editor/editor.main.css $@",
)

esbuild(
    name = "monaco_editor_worker_bundle",
    bazel_sandbox_plugin = False,
    entry_point = "monaco_editor_worker.js",
    metafile = False,
    minify = select({
        "//:fastbuild": False,
        "//conditions:default": True,
    }),
    deps = ["//:node_modules/monaco-editor"],
)

genrule(
    name = "uplot_css",
    srcs = ["//:node_modules/uplot/dir"],
    outs = ["uPlot.min.css"],
    cmd_bash = "cp $(SRCS)/dist/uPlot.min.css $@",
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [
        "index.js",
        "monaco_editor.main.css",
        "uPlot.min.css",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/bundle",
)
