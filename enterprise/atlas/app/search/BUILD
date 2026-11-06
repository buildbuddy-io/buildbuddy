load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "search",
    srcs = ["search.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/components/spinner",
        "//app/util:errors",
        "//enterprise/atlas/app/components:entry_row",
        "//enterprise/atlas/app/lib:rpc_service",
        "//proto:atlas_ts_proto",
    ],
)
