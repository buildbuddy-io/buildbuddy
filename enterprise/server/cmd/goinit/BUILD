load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("//rules:platform_transitions.bzl", "linux_arm64_musl_alias", "linux_x86_64_musl_alias")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(srcs = ["main.go"])

go_library(
    name = "goinit_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/goinit",
    deps = [
        "//enterprise/server/util/firecrackerutil",
        "//enterprise/server/util/vsock",
        "//enterprise/server/vmdns",
        "//enterprise/server/vmexec",
        "//enterprise/server/vmvfs:vmvfs_lib",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/rlimit",
        "//server/util/status",
        "@com_github_jsimonetti_rtnetlink//rtnl",
        "@com_github_miekg_dns//:dns",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "goinit",
    embed = [":goinit_lib"],
)

# The initrd embeds a statically linked goinit. Build it through the same
# musl platform transition used for the embedded bb CLI (//server/util/bb)
# instead of pure = "on": the resulting binary is still fully static, but it
# now shares its Bazel configuration (stdlib + ~800 shared packages) with bb
# instead of requiring a separate CGO_ENABLED=0 configuration.
linux_x86_64_musl_alias(
    name = "goinit_linux_x86_64_static",
    actual = ":goinit",
    tags = ["manual"],
)

linux_arm64_musl_alias(
    name = "goinit_linux_arm64_static",
    actual = ":goinit",
    tags = ["manual"],
)

alias(
    name = "goinit_static",
    actual = select({
        "//platforms/configs:linux_x86_64": ":goinit_linux_x86_64_static",
        "//platforms/configs:linux_arm64": ":goinit_linux_arm64_static",
    }),
)
