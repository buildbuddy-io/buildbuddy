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
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-virtualized-auto-sizer",
        "//:node_modules/@types/react-window",
        "//:node_modules/lucide-react",
        "//:node_modules/memoize-one",
        "//:node_modules/react",
        "//:node_modules/react-virtualized-auto-sizer",
        "//:node_modules/react-window",
        "//:node_modules/tslib",
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
    deps = ["//:node_modules/tslib"],
)

ts_library(
    name = "row",
    srcs = ["row.tsx"],
    deps = [
        "//app/router",
        "//app/terminal:text",
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-window",
        "//:node_modules/react",
        "//:node_modules/react-window",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "text",
    srcs = ["text.tsx"],
    deps = [
        "//app/terminal:ansi",
        "//:node_modules/memoize-one",
        "//:node_modules/tslib",
    ],
)
