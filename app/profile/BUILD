load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["profile.css"])

ts_library(
    name = "profile",
    srcs = ["profile.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/invocation:invocation_breakdown_card",
        "//app/trace:timing_profile_drop_target",
        "//app/trace:trace_events",
        "//app/trace:trace_viewer",
    ],
)
