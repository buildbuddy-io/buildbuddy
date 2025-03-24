load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@npm//@bazel/esbuild:index.bzl", "esbuild")

package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "index_js",
    srcs = ["//enterprise/tools/databuddy/app:app_bundle"],
    outs = ["index.js"],
    cmd_bash = "cp $(SRCS)/index.js $@",
)

genrule(
    name = "monaco_css",
    srcs = ["@npm//:node_modules/monaco-editor/min/vs/editor/editor.main.css"],
    outs = ["monaco_editor.main.css"],
    cmd_bash = "echo $(SRCS); cp $(SRCS) $@",
)

esbuild(
    name = "monaco_editor_worker_bundle",
    entry_points = ["@npm//:node_modules/monaco-editor/esm/vs/editor/editor.worker.js"],
    metafile = False,
    minify = select({
        "//:fastbuild": False,
        "//conditions:default": True,
    }),
    deps = ["@npm//monaco-editor:monaco-editor__all_files"],
)

genrule(
    name = "monaco_editor_worker_js",
    srcs = [":monaco_editor_worker_bundle"],
    outs = ["monaco_editor.worker.js"],
    cmd_bash = "cp $(SRCS)/editor.worker.js $@",
)

genrule(
    name = "uplot_css",
    srcs = ["@npm//:node_modules/uplot/dist/uPlot.min.css"],
    outs = ["uPlot.min.css"],
    cmd_bash = "cp $(SRCS) $@",
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [
        "index.js",
        "monaco_editor.main.css",
        "monaco_editor.worker.js",
        "uPlot.min.css",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/bundle",
)
