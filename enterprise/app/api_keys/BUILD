load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "api_keys",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//app/util:errors",
        "//proto:api_key_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
