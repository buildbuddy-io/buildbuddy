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
        "@com_github_charmbracelet_bubbles//table",
        "@com_github_charmbracelet_bubbles//viewport",
        "@com_github_charmbracelet_bubbletea//:bubbletea",
        "@com_github_charmbracelet_lipgloss//:lipgloss",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
