load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["executors.css"])

ts_library(
    name = "executors",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:api_key_ts_proto",
        "//proto:bazel_config_ts_proto",
        "//proto:scheduler_ts_proto",
    ],
)
