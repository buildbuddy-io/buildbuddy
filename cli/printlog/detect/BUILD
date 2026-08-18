load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "detect",
    srcs = ["detect.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/printlog/detect",
    deps = [
        "//proto:remote_execution_log_go_proto",
        "//proto:spawn_go_proto",
        "@com_github_klauspost_compress//zstd",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)

go_test(
    name = "detect_test",
    srcs = ["detect_test.go"],
    data = [
        "//cli/explain/compactgraph/testdata:all_logs",
    ],
    deps = [
        ":detect",
        "//proto:remote_execution_go_proto",
        "//proto:remote_execution_log_go_proto",
        "//proto:spawn_go_proto",
        "@com_github_klauspost_compress//zstd",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//encoding/protodelim",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
