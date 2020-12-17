load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/db:go_default_library",
        "//server/util/random:go_default_library",
        "//server/util/request_context:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_coreos_go_oidc//:go_default_library",
        "@com_github_dgrijalva_jwt_go//:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@org_golang_google_grpc//credentials:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//peer:go_default_library",
        "@org_golang_x_oauth2//:go_default_library",
    ],
)
