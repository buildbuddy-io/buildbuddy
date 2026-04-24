load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "saml",
    srcs = ["saml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/saml",
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/cookie",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_crewjam_saml//:saml",
        "@com_github_crewjam_saml//samlsp",
        "@com_github_golang_jwt_jwt_v4//:jwt",
    ],
)

go_test(
    name = "saml_test",
    srcs = ["saml_test.go"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:group_go_proto",
        "//proto:user_go_proto",
        "//server/testutil/app",
        "//server/testutil/testport",
        "@com_github_beevik_etree//:etree",
        "@com_github_crewjam_saml//:saml",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
