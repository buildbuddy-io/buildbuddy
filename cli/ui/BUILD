load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "ui",
    srcs = [
        "api.go",
        "detail.go",
        "format.go",
        "list.go",
        "model.go",
        "orgpicker.go",
        "styles.go",
        "ui.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/ui",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:group_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:user_go_proto",
        "//server/util/grpc_client",
        "@land_charm_bubbles_v2//viewport",
        "@land_charm_bubbletea_v2//:bubbletea",
        "@land_charm_lipgloss_v2//:lipgloss",
        "@land_charm_lipgloss_v2//table",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
