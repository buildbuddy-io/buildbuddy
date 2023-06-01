load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "menu",
    srcs = ["menu.tsx"],
    strict = True,
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/preferences",
        "//app/router",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
