load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "encryption",
    srcs = ["encryption.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:encryption_ts_proto",
    ],
)

exports_files(glob(["*.css"]))
