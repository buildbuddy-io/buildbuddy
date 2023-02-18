load("//rules/typescript:index.bzl", "ts_library")

ts_library(
    name = "hero",
    srcs = ["hero.tsx"],
    deps = [
        "//website/src/components/hero:hero.module.css",
        "//website/src/css:common.module.css",
        "//website/src/util:clipboard",
        "@npm//@theme/IdealImage",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
