load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "auth_service",
    srcs = ["auth_service.ts"],
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//app/util:popup",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "//proto:user_ts_proto",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "user",
    srcs = ["user.ts"],
    deps = [
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "//proto:user_ts_proto",
    ],
)
