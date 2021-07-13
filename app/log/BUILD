load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["log.css"])

ts_library(
    name = "invocation_log",
    srcs = [
        "fake_fetcher.tsx",
        "fake_fetcher_data.tsx",
        "invocation_log.tsx",
    ],
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/errors",
        "//app/favicon",
        "//app/log",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:eventlog_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:user_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "log",
    srcs = ["log.tsx"],
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/favicon",
        "//app/router",
        "//app/service",
        "//proto:user_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
