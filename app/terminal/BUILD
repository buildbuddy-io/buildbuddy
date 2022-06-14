load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "terminal",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//proto:eventlog_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-lazylog",
        "//:node_modules/@types/react-virtualized",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/react-lazylog",
        "//:node_modules/react-virtualized",
    ],
)
