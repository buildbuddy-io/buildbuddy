load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "terminal",
    srcs = glob(["*.tsx"]),
    deps = [
        "@npm//@types/react",
        "@npm//@types/react-lazylog",
        "@npm//@types/react-virtualized",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//react-lazylog",
        "@npm//react-virtualized",
    ],
)
