load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "group_search",
    srcs = ["group_search.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/errors:error_service",
    ],
)
