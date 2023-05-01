load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "footer",
    srcs = ["footer.tsx"],
    strict = True,
    deps = [
        "//app/capabilities",
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)
