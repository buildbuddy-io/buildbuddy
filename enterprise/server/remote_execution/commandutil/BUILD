load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "commandutil",
    srcs = [
        "commandutil.go",
        "commandutil_unix.go",
        "commandutil_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil",
    deps = [
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mitchellh_go_ps//:go-ps",
    ] + select({
        "@io_bazel_rules_go//go/platform:windows": [
            "@com_github_shirou_gopsutil_v3//process",
            "@org_golang_x_sys//windows",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "commandutil_test",
    size = "enormous",
    srcs = [
        "commandutil_test.go",
        "commandutil_unix_test.go",
        "commandutil_windows_test.go",
    ],
    data = select({
        "@io_bazel_rules_go//go/platform:windows": [],
        "//conditions:default": ["//enterprise/server/remote_execution/commandutil/test_binary"],
    }),
    deps = [
        ":commandutil",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//server/testutil/testfs",
            "//server/util/status",
            "@com_github_stretchr_testify//assert",
            "@com_github_stretchr_testify//require",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/testutil/testfs",
            "//server/util/status",
            "@com_github_stretchr_testify//assert",
            "@com_github_stretchr_testify//require",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/testutil/testfs",
            "@com_github_stretchr_testify//assert",
        ],
        "//conditions:default": [],
    }),
)
