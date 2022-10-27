load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["secrets.css"])

ts_library(
    name = "secrets",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/alert",
        "//app/components/button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/spinner",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//proto:secrets_ts_proto",
        "@npm//@types/libsodium-wrappers",
        "@npm//@types/react",
        "@npm//libsodium-wrappers",
        "@npm//lucide-react",
        "@npm//path-browserify",
        "@npm//react",
    ],
)
