load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "error_service",
    srcs = ["error_service.ts"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/router",
        "//app/util:errors",
    ],
)
