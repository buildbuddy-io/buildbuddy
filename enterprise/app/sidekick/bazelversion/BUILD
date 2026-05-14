load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "bazelversion",
    srcs = ["bazelversion.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/select",
    ],
)
