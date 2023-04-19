load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "encryption",
    srcs = ["encryption.tsx"],
    strict = True,
    deps = [
        "//app/service:rpc_service",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

exports_files(glob(["*.css"]))
