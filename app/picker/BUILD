load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "picker",
    srcs = ["picker.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/components/modal",
        "//app/picker:picker_service",
    ],
)

exports_files(srcs = glob(["*.css"]))

ts_library(
    name = "picker_service",
    srcs = ["picker_service.ts"],
    deps = ["//:node_modules/rxjs"],
)
