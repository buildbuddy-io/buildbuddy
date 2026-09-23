load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "permissionstest",
    testonly = 1,
    srcs = ["permissionstest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest",
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testoidc",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/tables",
        "//server/testutil/app",
        "//server/util/proto",
        "//server/util/role",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_x_net//html",
    ],
)
