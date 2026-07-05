load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "date_range_picker",
    srcs = ["date_range_picker.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/button",
        "//app/components/input",
        "//app/components/select",
        "//app/format",
        "//app/util:date",
    ],
)

ts_library(
    name = "date_range_picker_button",
    srcs = ["date_range_picker_button.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/date_range_picker",
        "//app/components/popup",
        "//app/format",
        "//app/router",
        "//app/router:router_params",
        "//app/util:date",
    ],
)
