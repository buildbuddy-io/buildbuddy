load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "auth",
    srcs = glob(["*.ts"]),
    deps = [
        "//app/capabilities",
        "//app/service",
        "//proto:context_ts_proto",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "//proto:user_ts_proto",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)
