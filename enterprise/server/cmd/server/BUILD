load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_distroless//distroless:defs.bzl", "flatten")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("@tar.bzl", "mtree_mutate", "mtree_spec", "mutate", "tar")
load("//rules:utils.bzl", "runfiles_directory")

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
        "//enterprise/server/backends/metacache",
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
        "//enterprise/server/ip_rules_enforcer",
        "//enterprise/server/ip_rules_service",
        "//enterprise/server/mcp/mcpserver",
        "//enterprise/server/oci/ocifetcher",
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
        "//enterprise/server/splash",
        "//enterprise/server/suggestion",
        "//enterprise/server/tasksize",
        "//enterprise/server/telemetry",
        "//enterprise/server/usage",
        "//enterprise/server/usage_service",
        "//enterprise/server/util/dsingleflight",
        "//enterprise/server/util/redisutil",
        "//enterprise/server/util/trafficstats",
        "//enterprise/server/webhooks/bitbucket",
        "//enterprise/server/webhooks/github",
        "//enterprise/server/workflow/service",
        "//enterprise/server/workspace",
        "//server/config",
        "//server/interfaces",
        "//server/janitor",
        "//server/libmain",
        "//server/real_environment",
        "//server/remote_cache/capabilities_server",
        "//server/telemetry",
        "//server/util/clickhouse",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/tracing",
        "//server/version",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//stats",
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

PACKAGE_ROOT = "app/%s/buildbuddy.runfiles/%s" % (
    package_name(),
    runfiles_directory(),
)

package_runfiles = mutate(
    package_dir = PACKAGE_ROOT,
    strip_prefix = "%s/buildbuddy_/buildbuddy.runfiles/%s" % (
        package_name(),
        runfiles_directory(),
    ),
)

tar(
    name = "runfiles_tar",
    srcs = [":buildbuddy"],
    include_runfiles = True,
    mutate = package_runfiles,
)

pkg_tar(
    name = "symlinks_tar",
    symlinks = {
        "config.yaml": "/" + PACKAGE_ROOT + "/enterprise/config/buildbuddy.release.yaml",
        "buildbuddy": "tmp",
        "app/enterprise/server/cmd/server/buildbuddy": "/" + PACKAGE_ROOT + "/enterprise/server/cmd/server/buildbuddy_/buildbuddy",
    },
)

flatten(
    name = "layer",
    compress = "gzip",
    tars = [
        ":runfiles_tar",
        ":symlinks_tar",
    ],
)

image_manifest(
    name = "image_manifest",
    base = "//images/distroless/base-nossl:manifest",
    entrypoint = [
        "/app/enterprise/server/cmd/server/buildbuddy",
    ],
    layers = [
        ":layer",
    ],
)

image_load(
    name = "buildbuddy_image",
    # Explicitly use containerd for clarity. The container will be accessible
    # via `nerdctl` in the "moby" namespace.
    #
    # We use containerd, as advised by `rules_img`, to avoid `docker load`,
    # which, if the host system is not using the containerd backend for docker,
    # will create a cusom docker manifest.
    # Loading into containerd allows us to load the exact OCI image we've built
    # with the expected digest.
    daemon = "docker",
    image = ":image_manifest",
    tag = "buildbuddy_image:latest",
)
