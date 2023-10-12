load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = ["flame_chart.tsx"],
    deps = [
        "//app/capabilities",
        "//app/components/scrollbar",
        "//app/flame_chart:flame_chart_model",
        "//app/flame_chart:style_constants",
        "//app/trace:trace_events",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:dom",
        "//app/util:math",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "flame_chart_model",
    srcs = ["flame_chart_model.ts"],
    deps = [
        "//app/capabilities",
        "//app/flame_chart:style_constants",
        "//app/trace:trace_events",
        "//app/util:color",
        "@npm//tslib",
    ],
)

ts_library(
    name = "style_constants",
    srcs = ["style_constants.ts"],
)
