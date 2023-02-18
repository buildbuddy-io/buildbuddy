load("//rules/typescript:index.bzl", "ts_library")

ts_library(
    name = "oss-list",
    srcs = ["oss-list.tsx"],
    deps = [
        "//website/src/components/oss:oss.module.css",
        "//website/src/css:common.module.css",
        "@npm//@theme/IdealImage",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "oss",
    srcs = ["oss.tsx"],
    deps = [
        "//website/src/components/oss:oss.module.css",
        "//website/src/components/oss:oss-list",
        "//website/src/css:common.module.css",
        "@npm//@theme/IdealImage",
        "@npm//@types/react",
        "@npm//react",
    ],
)
