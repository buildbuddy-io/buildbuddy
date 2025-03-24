load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "iprules",
    srcs = ["iprules.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:group_ts_proto",
        "//proto:iprules_ts_proto",
        "@npm//@types/react",
        "@npm//date-fns",
        "@npm//react",
        "@npm//tslib",
    ],
)
