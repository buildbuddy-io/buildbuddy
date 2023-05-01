load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "alert",
    srcs = ["alert.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//app/alert:alert_service",
        "//app/components/banner",
    ],
)

exports_files(srcs = glob(["*.css"]))

ts_library(
    name = "alert_service",
    srcs = ["alert_service.ts"],
    strict = True,
    deps = ["//:node_modules/rxjs"],
)
