load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "date_range_picker",
    srcs = ["date_range_picker.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/date-fns",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/button",
        "//app/components/input",
        "//app/components/select",
        "//app/format",
    ],
)
