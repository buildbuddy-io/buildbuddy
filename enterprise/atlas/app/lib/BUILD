load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "config",
    srcs = ["config.ts"],
    deps = ["//proto:atlas_ts_proto"],
)

ts_library(
    name = "format",
    srcs = ["format.ts"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//app/util:proto",
        "//proto:atlas_ts_proto",
        "//proto:timestamp_ts_proto",
    ],
)

ts_library(
    name = "router",
    srcs = ["router.ts"],
    deps = ["//:node_modules/tslib"],
)

ts_library(
    name = "rpc_service",
    srcs = ["rpc_service.ts"],
    deps = [
        "//:node_modules/tslib",
        "//app/service:length_prefixed",
        "//app/util:errors",
        "//proto:atlas_ts_proto",
        "//proto:grpc_code_ts_proto",
    ],
)

ts_library(
    name = "theme",
    srcs = ["theme.ts"],
)
