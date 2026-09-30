load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Stress test for graceful shutdown of a real app process. Not run in CI; run
# manually with --@io_bazel_rules_go//go/config:race to catch data races
# during shutdown.
go_test(
    name = "scheduler_shutdown_test",
    size = "enormous",
    srcs = ["scheduler_shutdown_test.go"],
    data = [
        "//enterprise/config/test:buildbuddy.noauth.yaml",
        "//enterprise/server/cmd/server:buildbuddy",
    ],
    tags = ["manual"],
    x_defs = {
        "configRlocationpath": "$(rlocationpaths //enterprise/config/test:buildbuddy.noauth.yaml)",
        "serverRlocationpath": "$(rlocationpaths //enterprise/server/cmd/server:buildbuddy)",
    },
    deps = [
        "//enterprise/server/testutil/testexecutor",
        "//enterprise/server/testutil/testredis",
        "//proto:scheduler_go_proto",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
    ],
)
