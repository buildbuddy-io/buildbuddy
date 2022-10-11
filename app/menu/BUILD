load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "menu",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/protobufjs",
        "//:node_modules/react",
        "//app/auth",
        "//app/capabilities",
        "//app/preferences",
        "//app/router",
        "//app/service",
        "//proto:invocation_ts_proto",
        "//proto:user_ts_proto",
    ],
)
