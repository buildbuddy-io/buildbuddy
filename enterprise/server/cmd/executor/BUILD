load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_layer")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executor_lib",
    srcs = [
        "executor.go",
        "executor_linux.go",
        "executor_notlinux.go",
        "executor_unix.go",
        "executor_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/executor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/gcs_cache",
        "//enterprise/server/backends/memcache",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/backends/s3_cache",
        "//enterprise/server/clientidentity",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/executor",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/runner",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/scheduling/priority_task_scheduler",
        "//enterprise/server/scheduling/scheduler_client",
        "//enterprise/server/scheduling/task_leaser",
        "//enterprise/server/tasksize",
        "//enterprise/server/util/cpuset",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/config",
        "//server/environment",
        "//server/hostid",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/resources",
        "//server/ssl",
        "//server/util/canary",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/usageutil",
        "//server/util/vtprotocodec",
        "//server/version",
        "//server/xcode",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//encoding/gzip",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/cgroup",
            "//enterprise/server/remote_execution/vbd",
            "//server/util/networking",
        ],
        "//conditions:default": [],
    }),
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
        "//enterprise:licenses",
        "//enterprise/config:config_files",
    ],
    embed = [":executor_lib"],
)

container_layer(
    name = "tini_layer",
    directory = "/",
    files = select({
        "@platforms//cpu:x86_64": [
            "@com_github_krallin_tini_tini-linux-amd64//file:tini",
        ],
        "@platforms//cpu:arm64": [
            "@com_github_krallin_tini_tini-linux-arm64//file:tini",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

container_layer(
    name = "docker_credential_gcr_layer",
    directory = "/usr/bin",
    files = select({
        "@platforms//cpu:x86_64": [
            "@com_github_googlecloudplatform_docker-credential-gcr-linux-amd64//:docker-credential-gcr.bin",
        ],
        "@platforms//cpu:arm64": [
            "@com_github_googlecloudplatform_docker-credential-gcr-linux-arm64//:docker-credential-gcr.bin",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

container_layer(
    name = "docker_credential_gcr_docker_config_layer",
    directory = "/root/.docker",
    files = [
        "//enterprise/server/cmd/executor/docker_credential_gcr_config:config.json",
    ],
    tags = ["manual"],
)

container_layer(
    name = "containerd_layer",
    directory = "/usr/bin",
    files = select({
        "@platforms//cpu:x86_64": [
            "@com_github_containerd_containerd-linux-amd64//:containerd.bin",
            "@com_github_containerd_containerd-linux-amd64//:containerd-shim-runc-v2.bin",
            "@com_github_containerd_containerd-linux-amd64//:ctr.bin",
        ],
        "@platforms//cpu:arm64": [
            "@com_github_containerd_containerd-linux-arm64//:containerd.bin",
            "@com_github_containerd_containerd-linux-arm64//:containerd-shim-runc-v2.bin",
            "@com_github_containerd_containerd-linux-arm64//:ctr.bin",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

container_layer(
    name = "rootlesskit_layer",
    directory = "/usr/bin",
    files = select({
        "@platforms//cpu:x86_64": [
            # "@com_github_rootless_containers_rootlesskit-linux-amd64//:rootlessctl.bin",
            "@com_github_rootless_containers_rootlesskit-linux-amd64//:rootlesskit.bin",
        ],
        "@platforms//cpu:arm64": [
            # "@com_github_rootless_containers_rootlesskit-linux-arm64//:rootlessctl.bin",
            "@com_github_rootless_containers_rootlesskit-linux-arm64//:rootlesskit.bin",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

container_layer(
    name = "runc_layer",
    directory = "/usr/bin",
    files = select({
        "@platforms//cpu:x86_64": [
            "@com_github_opencontainers_runc_runc-linux-amd64//file:runc",
        ],
        "@platforms//cpu:arm64": [
            "@com_github_opencontainers_runc_runc-linux-arm64//file:runc",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

# Executor expects "firecracker" and "jailer" binaries in $PATH,
# and they can't be symlinks (otherwise the VM will not start).
# Rename the firecracker/jailer binaries so that we can place
# those into /usr/bin as regular files.

genrule(
    name = "firecracker_rename",
    srcs = select({
        "@platforms//cpu:arm64": ["@com_github_firecracker_microvm_firecracker_arm64//:firecracker"],
        "//conditions:default": ["@com_github_firecracker_microvm_firecracker//:firecracker"],
    }),
    outs = ["firecracker"],
    cmd = "cp $(SRCS) $@",
)

genrule(
    name = "jailer_rename",
    srcs = select({
        "@platforms//cpu:arm64": ["@com_github_firecracker_microvm_firecracker_arm64//:jailer"],
        "//conditions:default": ["@com_github_firecracker_microvm_firecracker//:jailer"],
    }),
    outs = ["jailer"],
    cmd = "cp $(SRCS) $@",
)

container_layer(
    name = "executor_tools_layer",
    directory = "/usr/bin",
    files = select({
        "@platforms//cpu:x86_64": [
            ":firecracker",
            ":jailer",
            "@com_github_buildbuddy_io_soci_snapshotter-static-linux-amd64//:soci-store.bin",
            "@com_github_buildbuddy_io_soci_snapshotter-static-linux-amd64//:soci-store-race.bin",
            "@com_github_containers_crun_crun-linux-amd64//file:crun",
        ],
        "@platforms//cpu:aarch64": [
            ":firecracker",
            ":jailer",
            "@com_github_containers_crun_crun-linux-arm64//file:crun",
        ],
        "//conditions:default": [],
    }),
    tags = ["manual"],
)

container_layer(
    name = "podman_static_layer",
    directory = "/",
    tags = ["manual"],
    tars = ["//enterprise/server/remote_execution/containers/podman:podman-static.tar.gz"],
)

container_image(
    name = "base_image",
    base = "@executor_image//image:dockerfile_image.tar",
    layers = [
        ":tini_layer",
        ":docker_credential_gcr_layer",
        ":docker_credential_gcr_docker_config_layer",
        ":containerd_layer",
        ":runc_layer",
        ":rootlesskit_layer",
        ":executor_tools_layer",
        ":podman_static_layer",
    ],
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
    name = "executor_go_image",
    base = ":base_image",
    binary = ":executor",
    tags = ["manual"],
)

container_image(
    name = "executor_image",
    base = ":executor_go_image",
    entrypoint = [
        "/tini",
        "--",
        "/app/enterprise/server/cmd/executor/executor",
    ],
    tags = ["manual"],
)

# This image is the docker image used by the executor to run actions that do not
# already have a docker image specified.
container_image(
    name = "default_base_image",
    base = "@default_execution_image//image:dockerfile_image.tar",
    tags = ["manual"],
)
