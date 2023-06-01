load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_layer")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executor_lib",
    srcs = ["executor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/executor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise:bundle",
        "//enterprise/server/auth",
        "//enterprise/server/backends/gcs_cache",
        "//enterprise/server/backends/memcache",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/backends/s3_cache",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/podman",
        "//enterprise/server/remote_execution/executor",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/runner",
        "//enterprise/server/scheduling/priority_task_scheduler",
        "//enterprise/server/scheduling/scheduler_client",
        "//enterprise/server/tasksize",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/config",
        "//server/environment",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/resources",
        "//server/ssl",
        "//server/util/fileresolver",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/tracing",
        "//server/xcode",
        "@com_github_google_uuid//:uuid",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//encoding/gzip",
        "@org_golang_google_grpc//test/bufconn",
    ],
)

go_binary(
    name = "executor",
    args = [
        "--config_file=enterprise/config/executor.local.yaml",
        "--port=8888",
        "--monitoring_port=9091",
        "--max_shutdown_duration=3s",
    ],
    data = [
        "//enterprise:config_files",
        "//enterprise:licenses",
    ],
    embed = [":executor_lib"],
)

# Executor expects "firecracker" and "jailer" binaries in $PATH,
# and they can't be symlinks (otherwise the VM will not start).
# Rename the firecracker/jailer binaries so that we can place
# those into /usr/bin as regular files.

genrule(
    name = "firecracker_rename",
    srcs = ["@com_github_firecracker_microvm_firecracker-v1.1.1-x86_64//:firecracker-v1.1.1-x86_64"],
    outs = ["firecracker"],
    cmd = "cp $(SRCS) $@",
)

genrule(
    name = "jailer_rename",
    srcs = ["@com_github_firecracker_microvm_firecracker-v1.1.1-x86_64//:jailer-v1.1.1-x86_64"],
    outs = ["jailer"],
    cmd = "cp $(SRCS) $@",
)

container_layer(
    name = "executor_tools",
    directory = "/usr/bin",
    files = [
        ":firecracker",
        ":jailer",
        "@com_github_iain_macdonald_soci_snapshotter-soci-store-v0.2.3-linux-amd64//file:soci-store-v0.2.3-linux-amd64",
    ],
    symlinks = {
        "/usr/bin/soci-store": "/usr/bin/soci-store-v0.2.3-linux-amd64",
    },
)

container_image(
    name = "base_image",
    base = "@executor_image//image:dockerfile_image.tar",
    layers = [":executor_tools"],
    symlinks = {
        "config.yaml": "app/enterprise/server/cmd/executor/executor.runfiles/buildbuddy/enterprise/config/executor.release.yaml",
    },
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
#
# This target can be run locally with enterprise/tools/run_executor_image.sh
go_image(
    name = "executor_image",
    base = ":base_image",
    binary = ":executor",
    tags = ["manual"],
)

# This image is the docker image used by the executor to run actions that do not
# already have a docker image specified.
container_image(
    name = "default_base_image",
    base = "@default_execution_image//image:dockerfile_image.tar",
    tags = ["manual"],
)
