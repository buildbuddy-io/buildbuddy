load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "download",
    srcs = ["download.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/download",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/terminal",
        "//proto:remote_execution_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/cache/dirtools",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/mdutil",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/uuid",
        "@com_github_docker_go_units//:go-units",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_test(
    name = "download_test",
    srcs = ["download_test.go"],
    embed = [":download"],
    deps = [
        "//proto/api/v1:api_v1_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
