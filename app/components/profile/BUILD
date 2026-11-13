load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

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
        "//app/components/select",
        "//app/components/spinner",
        "//app/format",
        "//app/util:clipboard",
        "//app/util:errors",
        "//app/util:proto",
        "//proto:duration_ts_proto",
        "//proto:profile_ts_proto",
    ],
)
