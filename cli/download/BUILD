load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "download",
    srcs = ["download.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/download",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/storage",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
