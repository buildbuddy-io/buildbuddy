load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["sidebar.css"])

ts_library(
    name = "sidebar",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/auth",
        "//app/capabilities",
        "//app/components/link",
        "//app/router",
        "//app/service",
    ],
)
