load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

# gazelle:default_visibility //enterprise:__subpackages__
package(
    default_visibility = [
        "//enterprise:__subpackages__",
    ],
)

go_library(
    name = "main",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner",
    deps = [
        "//enterprise/server/bes_artifacts",
        "//enterprise/server/build_event_publisher",
        "//enterprise/server/clientidentity",
        "//enterprise/server/workflow/config",
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/bazel",
        "//server/util/bazelisk",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@com_github_docker_go_units//:go-units",
        "@com_github_google_shlex//:shlex",
        "@com_github_google_uuid//:uuid",
        "@com_github_logrusorgru_aurora//:aurora",
        "@in_gopkg_yaml_v2//:yaml_v2",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "ci_runner",
    embed = [":main"],
    pure = "on",
    static = "on",
)

container_image(
    name = "base_image",
    base = "@ci_runner_image//image:dockerfile_image.tar",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "ci_runner_image",
    base = ":base_image",
    binary = ":ci_runner",
    tags = ["manual"],
)
