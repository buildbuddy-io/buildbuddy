load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "alert",
    srcs = ["alert.tsx"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/components/banner",
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
    ],
)

exports_files(srcs = glob(["*.css"]))

ts_library(
    name = "alert_service",
    srcs = ["alert_service.ts"],
    strict = True,
    deps = ["@npm//rxjs"],
)
