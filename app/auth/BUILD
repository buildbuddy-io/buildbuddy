load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "auth",
    srcs = ["auth_service.ts"],
    deps = [
        ":user",
        "//app/capabilities",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:context_ts_proto",
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
        "//app/service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "@npm//tslib",
    ],
)
