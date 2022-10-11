load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "alert",
    srcs = glob([
        "*.tsx",
        "*.ts",
    ]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
    ],
)

exports_files(srcs = glob(["*.css"]))
