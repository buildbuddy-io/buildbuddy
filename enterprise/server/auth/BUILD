load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:api_key_go_proto",
        "//server/config:go_default_library",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/capabilities:go_default_library",
        "//server/util/db:go_default_library",
        "//server/util/lru:go_default_library",
        "//server/util/random:go_default_library",
        "//server/util/request_context:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_coreos_go_oidc//:go_default_library",
        "@com_github_dgrijalva_jwt_go//:go_default_library",
        "@io_gorm_gorm//:go_default_library",
        "@org_golang_google_grpc//credentials:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//peer:go_default_library",
        "@org_golang_x_oauth2//:go_default_library",
    ],
)
