load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "menu",
    srcs = ["menu.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/preferences",
        "//app/router",
    ],
)
