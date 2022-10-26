load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["sidebar.css"])

ts_library(
    name = "sidebar",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/link",
        "//app/router",
        "//app/service",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
