load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = glob([
        "*.tsx",
        "*.ts",
    ]),
    deps = [
        "//app/components/scrollbar",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:dom",
        "//app/util:math",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
    ],
)
