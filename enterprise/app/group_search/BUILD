load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "group_search",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/errors",
        "//app/service",
        "@npm//@types/react",
        "@npm//react",
    ],
)
