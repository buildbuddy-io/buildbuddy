load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//enterprise/server/auth:go_default_library",
        "//enterprise/server/workflowconf:go_default_library",
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/util/grpc_client:go_default_library",
        "//server/util/lockingbuffer:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_go_git_go_git_v5//:go_default_library",
        "@com_github_go_git_go_git_v5//config:go_default_library",
        "@com_github_go_git_go_git_v5//plumbing:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_google_shlex//:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@com_github_logrusorgru_aurora//:go_default_library",
        "@in_gopkg_yaml_v2//:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_binary(
    name = "ci_runner",
    embed = [":go_default_library"],
    visibility = ["//enterprise:__subpackages__"],
)

# Note: the CI runner image only includes the setup needed for the CI runner;
# it doesn't include the CI runner binary itself. We ship the CI runner binary
# to the CAS and the executor downloads it as needed.
container_image(
    name = "ci_runner_image",
    base = "@ci_runner_image//image:dockerfile_image.tar",
    tags = ["manual"],
    visibility = ["//enterprise:__subpackages__"],
)
