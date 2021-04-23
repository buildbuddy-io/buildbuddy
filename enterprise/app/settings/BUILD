load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["settings.css"])

ts_library(
    name = "settings",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//enterprise/app/api_keys",
        "//enterprise/app/org",
        "@npm//@types/react",
        "@npm//react",
    ],
)
