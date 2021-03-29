load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//extras:embed_data.bzl", "go_embed_data")

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/fieldgetter",
        "//enterprise/server/webhooks/webhook_data",
        "//server/util/status",
        "@com_github_google_go_github//github",
    ],
)

go_embed_data(
    name = "data",
    testonly = 1,
    srcs = glob(["test_data/*.txt"]),
    package = "github",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
)

go_test(
    name = "github_test",
    srcs = ["github_test.go"],
    # keep
    embed = [
        ":github",
        ":data",
    ],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/webhooks/webhook_data",
        "@com_github_stretchr_testify//assert",
    ],
)
