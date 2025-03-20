load("//rules/typescript:index.bzl", "ts_library")

exports_files(glob(["*.css"]))

ts_library(
    name = "org_picker",
    srcs = ["org_picker.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/router",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
