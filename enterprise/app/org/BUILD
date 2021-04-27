load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "org",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/router",
        "//app/service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
