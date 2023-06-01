load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "button",
    srcs = ["button.tsx"],
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "button_group",
    srcs = ["button_group.tsx"],
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "checkbox_button",
    srcs = ["checkbox_button.tsx"],
    strict = True,
    deps = [
        "//app/components/button",
        "//app/components/checkbox",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "link_button",
    srcs = ["link_button.tsx"],
    strict = True,
    deps = [
        "//app/components/link",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)
