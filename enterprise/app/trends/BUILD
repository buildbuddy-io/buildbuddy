load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["trends.css"])

ts_library(
    name = "trends",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/rxjs",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:proto",
        "//enterprise/app/filter",
        "//proto:invocation_ts_proto",
    ],
)
