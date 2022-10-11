load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "terminal",
    srcs = glob(
        ["*.tsx"],
        exclude = glob(["_test.ts"]),
    ),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-virtualized-auto-sizer",
        "//:node_modules/@types/react-window",
        "//:node_modules/lucide-react",
        "//:node_modules/memoize-one",
        "//:node_modules/react",
        "//:node_modules/react-virtualized-auto-sizer",
        "//:node_modules/react-window",
        "//app/components/input",
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//app/util:math",
        "//app/util:scroller",
        "//proto:eventlog_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "text_test",
    srcs = ["text_test.ts"],
    deps = [":terminal"],
)
