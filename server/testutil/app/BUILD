load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "app",
    testonly = 1,
    srcs = ["app.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/app",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/testutil/testserver",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
        "@io_gorm_driver_sqlite//:sqlite",
        "@io_gorm_gorm//:gorm",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)
