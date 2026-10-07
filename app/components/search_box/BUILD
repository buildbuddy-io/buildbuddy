load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "completion",
    srcs = ["completion.ts"],
)

ts_library(
    name = "search_box",
    srcs = ["search_box.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/components/input",
        "//app/components/search_box:completion",
    ],
)
