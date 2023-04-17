load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "shortcuts",
    srcs = ["shortcuts.tsx"],
    strict = True,
    deps = [
        "//app/components/dialog",
        "//app/components/modal",
        "//app/preferences",
        "//app/shortcuts",
        "@npm//@types/react",
        "@npm//react",
    ],
)
