load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "profile",
    srcs = ["profile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/profile",
    deps = [
        "//proto:profile_go_proto",
        "//server/util/status",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "profile_test",
    srcs = ["profile_test.go"],
    embed = [":profile"],
    deps = [
        "//proto:profile_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
