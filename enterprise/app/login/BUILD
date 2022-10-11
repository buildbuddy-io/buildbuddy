load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "login",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/protobufjs",
        "//:node_modules/react",
        "//app/alert",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/input",
        "//app/router",
        "//app/service",
        "//proto:invocation_ts_proto",
    ],
)
