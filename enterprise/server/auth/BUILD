load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "auth",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:api_key_go_proto",
        "//server/config",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/capabilities",
        "//server/util/db",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/random",
        "//server/util/request_context",
        "//server/util/status",
        "//server/util/timeutil",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@com_github_dgrijalva_jwt_go//:jwt-go",
        "@org_golang_google_grpc//credentials",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//peer",
        "@org_golang_x_oauth2//:oauth2",
    ],
)

go_test(
    name = "auth_test",
    srcs = ["auth_test.go"],
    embed = [":auth"],
    deps = [
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/tables",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
