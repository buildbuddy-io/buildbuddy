load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "filter",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-date-range",
        "//:node_modules/date-fns",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/react-date-range",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/popup",
        "//app/components/radio",
        "//app/components/slider",
        "//app/format",
        "//app/router",
        "//app/util:proto",
        "//proto:invocation_ts_proto",
        "//proto:timestamp_ts_proto",
    ],
)

filegroup(
    name = "css",
    srcs = [
        ":filter.css",
        ":react_date_range_base.css",
        ":react_date_range_default_theme.css",
    ],
)
