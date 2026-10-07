load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "root",
    srcs = ["root.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/search_box",
        "//enterprise/atlas/app/clusters",
        "//enterprise/atlas/app/components:cluster_picker",
        "//enterprise/atlas/app/components:link",
        "//enterprise/atlas/app/components:theme_toggle",
        "//enterprise/atlas/app/lib:router",
        "//enterprise/atlas/app/lib:rpc_service",
        "//enterprise/atlas/app/lib:suggest",
        "//enterprise/atlas/app/lib:theme",
        "//enterprise/atlas/app/object",
        "//enterprise/atlas/app/search",
        "//proto:atlas_ts_proto",
    ],
)
