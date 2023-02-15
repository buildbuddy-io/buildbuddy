load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["workflows.css"])

ts_library(
    name = "workflows",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//app/alert",
        "//app/auth",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//app/util:clipboard",
        "//app/util:errors",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
    ],
)
