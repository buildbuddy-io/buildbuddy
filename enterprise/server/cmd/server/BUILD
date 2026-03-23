load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_distroless//distroless:defs.bzl", "flatten")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "layer_from_tar")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
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

BINARY_NAME = "buildbuddy"

filegroup(
    name = "licenses",
    data = [
        "//enterprise:licenses",
    ],
)

filegroup(
    name = "runfiles",
    data = [
        "//enterprise/app:app_bundle",
        "//enterprise/app:sha",
        "//enterprise/app:style",
        "//static",
    ],
)

filegroup(
    name = "config_files",
    data = [
        "//enterprise/config:config_files",
    ],
)

go_binary(
    name = BINARY_NAME,
    args = [
        "--config_file=enterprise/config/buildbuddy.local.yaml",
        "--max_shutdown_duration=3s",
        "--static_directory=static",
        "--app_directory=/enterprise/app",
    ],
    data = [
        ":config_files",
        ":runfiles",
    ],
    embed = [":server_lib"],
)

PACKAGE_ROOT = "app"

pkg_tar(
    name = "licenses_tar",
    srcs = [
        ":licenses",
    ],
    include_runfiles = True,
    package_dir = PACKAGE_ROOT,
    remap_paths = {
        ".runfiles/": "%s.runfiles/" % BINARY_NAME,
    },
)

pkg_tar(
    name = "binary_tar",
    srcs = [":buildbuddy"],
    package_dir = PACKAGE_ROOT,
)

pkg_tar(
    name = "runfiles_tar",
    srcs = [
        ":runfiles",
    ],
    include_runfiles = True,
    package_dir = PACKAGE_ROOT,
    remap_paths = {
        ".runfiles/": "%s.runfiles/" % BINARY_NAME,
    },
)

pkg_tar(
    name = "symlinks_tar",
    symlinks = {
        "buildbuddy": "tmp",
    },
)

flatten(
    name = "base_image_tar",
    tars = [
        ":licenses_tar",
        ":binary_tar",
        ":runfiles_tar",
        ":symlinks_tar",
    ],
)

layer_from_tar(
    name = "base_image_layer",
    src = "base_image_tar",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Server (unconfigured)",
        "org.opencontainers.image.description": (
            "Contains the BuildBuddy binary, runfiles, and symlinks " +
            "necessary to run the enterprise BuildBuddy server. Does not " +
            "include configuration files."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    compress = "gzip",
)

image_manifest(
    name = "base_image_manifest",
    base = "//images/distroless/base-nossl:manifest",
    entrypoint = [
        "/%s/%s" % (
            PACKAGE_ROOT,
            BINARY_NAME,
        ),
    ],
    layers = [
        ":base_image_layer",
    ],
)

pkg_tar(
    name = "config_files_tar",
    srcs = [
        ":config_files",
    ],
    include_runfiles = True,
    package_dir = PACKAGE_ROOT,
    remap_paths = {
        ".runfiles/": "%s.runfiles/" % BINARY_NAME,
    },
)

pkg_tar(
    name = "config_symlink_tar",
    symlinks = {
        "config.yaml": "%s/%s.runfiles/%s/enterprise/config/buildbuddy.release.yaml" % (
            PACKAGE_ROOT,
            BINARY_NAME,
            runfiles_directory(),
        ),
    },
)

flatten(
    name = "config_tar",
    tars = [
        ":config_files_tar",
        ":config_symlink_tar",
    ],
)

layer_from_tar(
    name = "config_layer",
    src = "config_tar",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Configuration (FOSS)",
        "org.opencontainers.image.description": (
            "Contains the FOSS BuildBuddy enterprise configuration files."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
)

image_manifest(
    name = "image_manifest",
    base = ":base_image_manifest",
    layers = [
        ":config_layer",
    ],
)

image_load(
    name = "buildbuddy_image",
    # Explicitly use containerd for clarity. The container will be accessible
    # via `nerdctl` in the "moby" namespace.
    #
    # We use containerd, as advised by `rules_img`, to avoid `docker load`,
    # which, if the host system is not using the containerd backend for docker,
    # will create a custom docker manifest.
    # Loading into containerd allows us to load the exact OCI image we've built
    # with the expected digest.
    daemon = "containerd",
    image = ":image_manifest",
    tag = "buildbuddy_image:latest",
)
