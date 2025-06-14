load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

# gazelle:default_visibility //enterprise:__subpackages__,@buildbuddy_internal//:__subpackages__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "@buildbuddy_internal//:__subpackages__",
])

go_library(
    name = "server_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/server",
    deps = [
        "//enterprise/app:bundle",
        "//enterprise/server/api",
        "//enterprise/server/auditlog",
        "//enterprise/server/auth",
        "//enterprise/server/auth_service",
        "//enterprise/server/backends/authdb",
        "//enterprise/server/backends/codesearch",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/gcs_cache",
        "//enterprise/server/backends/kms",
        "//enterprise/server/backends/memcache",
        "//enterprise/server/backends/migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/backends/prom",
        "//enterprise/server/backends/pubsub",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/backends/redis_client",
        "//enterprise/server/backends/redis_execution_collector",
        "//enterprise/server/backends/redis_kvstore",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/backends/s3_cache",
        "//enterprise/server/backends/userdb",
        "//enterprise/server/clientidentity",
        "//enterprise/server/crypter_service",
        "//enterprise/server/execution_search_service",
        "//enterprise/server/execution_service",
        "//enterprise/server/experiments",
        "//enterprise/server/gcplink",
        "//enterprise/server/githubapp",
        "//enterprise/server/hit_tracker_service",
        "//enterprise/server/hostedrunner",
        "//enterprise/server/invocation_search_service",
        "//enterprise/server/invocation_stat_service",
        "//enterprise/server/iprules",
        "//enterprise/server/ociregistry",
        "//enterprise/server/quota",
        "//enterprise/server/registry",
        "//enterprise/server/remote_execution/execution_server",
        "//enterprise/server/remote_execution/redis_client",
        "//enterprise/server/remote_execution/snaploader",
        "//enterprise/server/scheduling/scheduler_server",
        "//enterprise/server/scheduling/task_router",
        "//enterprise/server/scim",
        "//enterprise/server/secrets",
        "//enterprise/server/selfauth",
        "//enterprise/server/server_notification",
        "//enterprise/server/sociartifactstore",
        "//enterprise/server/splash",
        "//enterprise/server/suggestion",
        "//enterprise/server/tasksize",
        "//enterprise/server/telemetry",
        "//enterprise/server/usage",
        "//enterprise/server/usage_service",
        "//enterprise/server/util/dsingleflight",
        "//enterprise/server/util/redisutil",
        "//enterprise/server/webhooks/bitbucket",
        "//enterprise/server/webhooks/github",
        "//enterprise/server/workflow/service",
        "//enterprise/server/workspace",
        "//server/config",
        "//server/interfaces",
        "//server/janitor",
        "//server/libmain",
        "//server/real_environment",
        "//server/telemetry",
        "//server/util/clickhouse",
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
        "//enterprise:licenses",
        "//enterprise/app:app_bundle",
        "//enterprise/app:sha",
        "//enterprise/app:style",
        "//enterprise/config:config_files",
        "//static",
    ],
    embed = [":server_lib"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    symlinks = {
        "config.yaml": "app/enterprise/server/cmd/server/buildbuddy.runfiles/buildbuddy/enterprise/config/buildbuddy.release.yaml",
        "buildbuddy": "tmp",
    },
    tags = ["manual"],
)

# The go_image below can be used to build and run the server image.
# The container_image target just builds and tags the image without running it.

go_image(
    name = "buildbuddy_go_image",
    base = ":base_image",
    binary = ":buildbuddy",
    tags = ["manual"],
)

container_image(
    name = "buildbuddy_image",
    base = ":buildbuddy_go_image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# TODO(sluongng): Verify the layering of the image.
pkg_tar(
    name = "tar",
    srcs = [
        ":buildbuddy",
    ],
    include_runfiles = True,
    remap_paths = {
        "/buildbuddy": "/app/enterprise/server/cmd/server/buildbuddy",
    },
    symlinks = {
        "config.yaml": "app/enterprise/server/cmd/server/buildbuddy.runfiles/buildbuddy/enterprise/config/buildbuddy.release.yaml",
        "buildbuddy": "tmp",
    },
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/app/enterprise/server/cmd/server/buildbuddy"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)

oci_load(
    name = "oci_load",
    image = ":oci_image",
    repo_tags = ["enterprise/server:oci_image"],
    tags = ["manual"],
)
