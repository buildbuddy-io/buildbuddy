load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "encryption",
    srcs = ["encryption.tsx"],
    strict = True,
    deps = [
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:encryption_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

exports_files(glob(["*.css"]))
