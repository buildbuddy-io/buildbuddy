load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "shortcuts",
    srcs = ["shortcuts.tsx"],
    strict = False,
    deps = [
        "//app/preferences",
        "@npm//@types/uuid",
        "@npm//uuid",
    ],
)
