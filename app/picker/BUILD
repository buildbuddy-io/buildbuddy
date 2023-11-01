load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "picker",
    srcs = ["picker.tsx"],
    deps = [
        "//app/components/modal",
        "//app/picker:picker_service",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
    ],
)

exports_files(srcs = glob(["*.css"]))

ts_library(
    name = "picker_service",
    srcs = ["picker_service.ts"],
    deps = ["@npm//rxjs"],
)
