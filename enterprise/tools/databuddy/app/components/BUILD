load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "Collapsible",
    srcs = ["Collapsible.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "Panel",
    srcs = ["Panel.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//enterprise/tools/databuddy/app/components:Collapsible",
    ],
)

ts_library(
    name = "Editor",
    srcs = ["Editor.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//enterprise/tools/databuddy/app:monaco_config",
    ],
)

ts_library(
    name = "Plot",
    srcs = ["Plot.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//:node_modules/uplot",
    ],
)

ts_library(
    name = "Spinner",
    srcs = ["Spinner.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)
