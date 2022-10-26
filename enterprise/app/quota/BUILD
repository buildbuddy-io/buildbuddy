load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "quota",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/alert",
        "//app/components/button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:proto",
        "//proto:quota_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
