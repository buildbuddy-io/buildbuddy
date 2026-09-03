load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# go-nvml uses glibc-specific dynamic loader APIs.
config_setting(
    name = "linux_musl",
    constraint_values = [
        "@llvm//constraints/libc:musl",
        "@platforms//os:linux",
    ],
)

go_library(
    name = "gpu",
    srcs = ["gpu.go"] + select({
        ":linux_musl": ["gpu_musl.go"],
        "//conditions:default": [
            "gpu_linux.go",
            "gpu_unsupported.go",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/gpu",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/flag",
    ] + select({
        ":linux_musl": [],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/cgroup",
            "//server/util/log",
            "@com_github_nvidia_go_nvml//pkg/nvml",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "gpu_test",
    srcs = ["gpu_linux_test.go"],
    embed = [":gpu"],
    target_compatible_with = select({
        ":linux_musl": ["@platforms//:incompatible"],
        "@io_bazel_rules_go//go/platform:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = select({
        ":linux_musl": [],
        "@io_bazel_rules_go//go/platform:linux": [
            "//proto:remote_execution_go_proto",
            "//server/util/testing/flags",
            "@com_github_google_go_cmp//cmp",
            "@com_github_nvidia_go_nvml//pkg/nvml",
            "@com_github_nvidia_go_nvml//pkg/nvml/mock",
            "@com_github_stretchr_testify//require",
            "@org_golang_google_protobuf//testing/protocmp",
        ],
        "//conditions:default": [],
    }),
)
