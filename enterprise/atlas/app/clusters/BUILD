load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "clusters",
    srcs = ["clusters.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/spinner",
        "//app/util:errors",
        "//enterprise/atlas/app/components:badges",
        "//enterprise/atlas/app/lib:rpc_service",
        "//proto:atlas_ts_proto",
    ],
)
