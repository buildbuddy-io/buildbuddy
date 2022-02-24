load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:math",
        "//enterprise/app/filter",
        "//proto:invocation_ts_proto",
        "//proto:target_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//react-dom",
        "@npm//recharts",
        "@npm//rxjs",
    ],
)
