load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "link",
    srcs = ["link.tsx"],
    deps = [
        "//app/router",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

exports_files(["link.css"])
