load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "shortcuts",
    srcs = ["shortcuts.tsx"],
    deps = [
        "//app/preferences",
        "//:node_modules/@types/uuid",
        "//:node_modules/uuid",
    ],
)
