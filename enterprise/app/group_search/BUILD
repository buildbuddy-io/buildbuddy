load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "group_search",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/errors",
        "//app/service",
        "//proto:group_ts_proto",
    ],
)
