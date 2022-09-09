load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

go_library(
    name = "server_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise:bundle",
        "//enterprise/server/api",
        "//enterprise/server/auth",
        "//enterprise/server/backends/authdb",
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/gcs_cache",
        "//enterprise/server/backends/memcache",
        "//enterprise/server/backends/migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/backends/redis_client",
        "//enterprise/server/backends/redis_kvstore",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/backends/s3_cache",
        "//enterprise/server/backends/userdb",
        "//enterprise/server/execution_service",
        "//enterprise/server/hostedrunner",
        "//enterprise/server/invocation_search_service",
        "//enterprise/server/invocation_stat_service",
        "//enterprise/server/quota",
        "//enterprise/server/raft/cache",
        "//enterprise/server/registry:registry_lib",
        "//enterprise/server/remote_execution/execution_server",
        "//enterprise/server/remote_execution/redis_client",
        "//enterprise/server/saml",
        "//enterprise/server/scheduling/scheduler_server",
        "//enterprise/server/scheduling/task_router",
        "//enterprise/server/selfauth",
        "//enterprise/server/splash",
        "//enterprise/server/tasksize",
        "//enterprise/server/telemetry",
        "//enterprise/server/usage",
        "//enterprise/server/usage_service",
        "//enterprise/server/webhooks/bitbucket",
        "//enterprise/server/webhooks/github",
        "//enterprise/server/workflow/service",
        "//server/config",
        "//server/interfaces",
        "//server/janitor",
        "//server/libmain",
        "//server/real_environment",
        "//server/static",
        "//server/telemetry",
        "//server/util/clickhouse",
        "//server/util/fileresolver",
        "//server/util/flagutil",
        "//server/util/flagutil/yaml",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "buildbuddy",
    args = [
        "--config_file=enterprise/config/buildbuddy.local.yaml",
        "--max_shutdown_duration=3s",
        "--static_directory=static",
        "--app_directory=/enterprise/app",
    ],
    data = [
        "//:VERSION",
        "//enterprise:config_files",
        "//enterprise:licenses",
        "//enterprise/app:app_bundle",
        "//enterprise/app:sha",
        "//enterprise/app:style",
        "//static",
    ],
    embed = [":server_lib"],
    visibility = ["//visibility:public"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    symlinks = {
        "config.yaml": "app/enterprise/server/cmd/server/buildbuddy.runfiles/buildbuddy/enterprise/config/buildbuddy.release.yaml",
    },
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "buildbuddy_image",
    base = ":base_image",
    binary = ":buildbuddy",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
