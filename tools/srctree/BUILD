load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "srctree_lib",
    srcs = [
        "fetch.go",
        "graph.go",
        "srctree.go",
        "targets.go",
        "ui.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/srctree",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:spawn_go_proto",
        "//proto:stat_filter_go_proto",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_docker_go_units//:go-units",
        "@com_github_klauspost_compress//zstd",
        "@land_charm_bubbletea_v2//:bubbletea",
        "@land_charm_lipgloss_v2//:lipgloss",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_term//:term",
    ],
)

go_binary(
    name = "srctree",
    embed = [":srctree_lib"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "srctree_test",
    srcs = [
        "graph_test.go",
        "srctree_test.go",
        "targets_test.go",
        "ui_test.go",
        "walk_test.go",
    ],
    data = [
        "example-cel.binpb.zst",
        "example-cel-2.binpb.zst",
    ],
    embed = [":srctree_lib"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:spawn_go_proto",
        "@com_github_docker_go_units//:go-units",
        "@com_github_klauspost_compress//zstd",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@land_charm_bubbletea_v2//:bubbletea",
        "@land_charm_lipgloss_v2//:lipgloss",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//encoding/protodelim",
    ],
)
