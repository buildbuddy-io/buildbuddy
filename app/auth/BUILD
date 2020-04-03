load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "auth",
    srcs = glob(["*.ts"]),
    deps = [
        "//app/capabilities",
        "//app/service",
        "//proto:buildbuddy_service_ts_proto",
        "//proto:user_ts_proto",
        "@npm//@types/fbemitter",
        "@npm//fbemitter",
        "@npm//oidc-client",
        "@npm//protobufjs",
    ],
)
