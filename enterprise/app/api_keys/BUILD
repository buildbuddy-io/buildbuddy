load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "api_keys",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/errors",
        "//app/service",
        "//app/util:errors",
        "//proto:api_key_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
