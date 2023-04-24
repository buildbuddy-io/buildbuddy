load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "terminal",
    srcs = ["terminal.tsx"],
    strict = True,
    deps = [
        "//app/components/input",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/terminal:row",
        "//app/terminal:text",
        "//app/util:math",
        "//app/util:scroller",
        "@npm//@types/react",
        "@npm//@types/react-virtualized-auto-sizer",
        "@npm//@types/react-window",
        "@npm//lucide-react",
        "@npm//memoize-one",
        "@npm//react",
        "@npm//react-virtualized-auto-sizer",
        "@npm//react-window",
        "@npm//tslib",
    ],
)

ts_jasmine_node_test(
    name = "text_test",
    srcs = ["text_test.ts"],
    strict = True,
    deps = [":terminal"],
)

ts_library(
    name = "ansi",
    srcs = ["ansi.tsx"],
    deps = ["@npm//tslib"],
)

ts_library(
    name = "row",
    srcs = ["row.tsx"],
    deps = [
        "//app/router",
        "//app/terminal:text",
        "@npm//@types/react",
        "@npm//@types/react-window",
        "@npm//react",
        "@npm//react-window",
        "@npm//tslib",
    ],
)

ts_library(
    name = "text",
    srcs = ["text.tsx"],
    deps = [
        "//app/terminal:ansi",
        "@npm//memoize-one",
        "@npm//tslib",
    ],
)
