load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "terminal",
    srcs = glob(
        ["*.tsx"],
        exclude = glob(["_test.ts"]),
    ),
    strict = True,
    deps = [
        "//app/components/input",
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//app/util:math",
        "//app/util:scroller",
        "//proto:eventlog_ts_proto",
        "@npm//@types/react",
        "@npm//@types/react-virtualized-auto-sizer",
        "@npm//@types/react-window",
        "@npm//lucide-react",
        "@npm//memoize-one",
        "@npm//react",
        "@npm//react-virtualized-auto-sizer",
        "@npm//react-window",
    ],
)

ts_jasmine_node_test(
    name = "text_test",
    srcs = ["text_test.ts"],
    strict = True,
    deps = [":terminal"],
)
