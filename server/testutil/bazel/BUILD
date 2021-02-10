load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "wget_bazel-3.7.0-linux-x86_64",
    outs = ["bazel-3.7.0-linux-x86_64"],
    cmd_bash = """
    wget -O "$@" https://github.com/bazelbuild/bazel/releases/download/3.7.0/bazel-3.7.0-linux-x86_64
    [[ $$(sha256sum "$@" | awk '{print $$1}') == "b7583eec83cc38302997098a40b8c870c37e0ab971a83cb3364c754a178b74ec" ]] || exit 1
    """,
    visibility = ["//visibility:public"],
)

go_library(
    name = "go_default_library",
    srcs = ["bazel.go"],
    data = [":bazel-3.7.0-linux-x86_64"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/bazel",
    visibility = ["//visibility:public"],
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
