load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "group_search",
    srcs = ["group_search.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)
