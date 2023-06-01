load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = ["flame_chart.tsx"],
    strict = True,
    deps = [
        "//app/capabilities",
        "//app/components/scrollbar",
        "//app/flame_chart:flame_chart_model",
        "//app/flame_chart:profile_model",
        "//app/flame_chart:style_constants",
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

ts_jasmine_node_test(
    name = "profile_model_test",
    srcs = ["profile_model_test.ts"],
    deps = [":flame_chart"],
)

ts_library(
    name = "flame_chart_model",
    srcs = ["flame_chart_model.ts"],
    strict = True,
    deps = [
        "//app/capabilities",
        "//app/flame_chart:profile_model",
        "//app/flame_chart:style_constants",
        "//app/util:color",
        "@npm//tslib",
    ],
)

ts_library(
    name = "profile_model",
    srcs = ["profile_model.ts"],
    strict = True,
)

ts_library(
    name = "style_constants",
    srcs = ["style_constants.ts"],
    strict = True,
)
