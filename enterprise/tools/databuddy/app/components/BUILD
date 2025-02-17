load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "Collapsible",
    srcs = ["Collapsible.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "Panel",
    srcs = ["Panel.tsx"],
    deps = [
        "//enterprise/tools/databuddy/app/components:Collapsible",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "Editor",
    srcs = ["Editor.tsx"],
    deps = [
        "//enterprise/tools/databuddy/app:monaco_config",
        "@npm//@types/react",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "Plot",
    srcs = ["Plot.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
        "@npm//uplot",
    ],
)
