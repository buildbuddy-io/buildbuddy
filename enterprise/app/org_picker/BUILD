load("//rules/typescript:index.bzl", "ts_library")

exports_files(glob(["*.css"]))

ts_library(
    name = "org_picker",
    srcs = ["org_picker.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/router",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
