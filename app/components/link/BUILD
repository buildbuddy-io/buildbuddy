load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "blob",
    srcs = ["blob.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "link",
    srcs = ["link.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/router",
    ],
)

ts_jasmine_node_test(
    name = "link_test",
    srcs = ["link_test.ts"],
    deps = [
        ":link",
    ],
)

exports_files(["link.css"])
