load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gpu",
    srcs = [
        "gpu.go",
        "gpu_linux.go",
        "gpu_unsupported.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/gpu",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/flag",
    ] + select({
        # Static builds compile gpu_unsupported.go instead of gpu_linux.go,
        # because go-nvml needs glibc-only dlfcn symbols that musl lacks.
        "//platforms/configs:linux_static": [],
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
        "@io_bazel_rules_go//go/platform:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = select({
        # gpu_linux_test.go is excluded from static builds along with gpu_linux.go.
        "//platforms/configs:linux_static": [],
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
