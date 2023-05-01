load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "link",
    srcs = ["link.tsx"],
    strict = True,
    deps = [
        "//app/router",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

exports_files(["link.css"])
