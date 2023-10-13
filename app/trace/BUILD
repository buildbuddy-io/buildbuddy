load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "event_hovercard",
    srcs = ["event_hovercard.tsx"],
    deps = [
        "//app/trace:trace_events",
        "//app/util:math",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "trace_viewer",
    srcs = ["trace_viewer.tsx"],
    deps = [
        "//app/trace:constants",
        "//app/trace:event_hovercard",
        "//app/trace:trace_events",
        "//app/trace:trace_viewer_model",
        "//app/trace:trace_viewer_panel",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:dom",
        "//app/util:math",
        "//app/util:platform",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "trace_viewer_model",
    srcs = ["trace_viewer_model.ts"],
    deps = [
        "//app/trace:constants",
        "//app/trace:trace_events",
        "//app/util:color",
        "@npm//tslib",
    ],
)

exports_files(glob(["*.css"]))

ts_library(
    name = "constants",
    srcs = ["constants.ts"],
)

ts_library(
    name = "trace_events",
    srcs = ["trace_events.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_jasmine_node_test(
    name = "trace_events_test",
    srcs = ["trace_events_test.ts"],
    deps = [":trace_events"],
)

ts_library(
    name = "trace_viewer_panel",
    srcs = ["trace_viewer_panel.ts"],
    deps = [
        "//app/trace:constants",
        "//app/trace:trace_events",
        "//app/trace:trace_viewer_model",
        "//app/util:canvas",
        "//app/util:dom",
        "//app/util:math",
        "@npm//tslib",
    ],
)
