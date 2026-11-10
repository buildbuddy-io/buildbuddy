load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_layer")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")
load("@rules_distroless//distroless:defs.bzl", "flatten")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "image_layer", "layer_from_tar")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//images:common.bzl", "EXECUTOR_PACKAGE_ROOT")
load(
    "//rules:platform_transitions.bzl",
    "linux_arm64_alias",
    "linux_arm64_musl_alias",
    "linux_x86_64_alias",
    "linux_x86_64_musl_alias",
    "macos_arm64_alias",
    "macos_x86_64_alias",
    "windows_386_alias",
    "windows_amd64_alias",
    "windows_arm64_alias",
)
load("//rules:utils.bzl", "runfiles_directory")

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
        "//enterprise/server/remote_execution/executor/oomkiller",
        "//enterprise/server/remote_execution/executorplatform",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/gpu",
        "//enterprise/server/remote_execution/runner",
        "//enterprise/server/remote_execution/snaputil",
        "//enterprise/server/scheduling/priority_task_scheduler",
        "//enterprise/server/scheduling/scheduler_client",
        "//enterprise/server/scheduling/task_leaser",
        "//enterprise/server/tasksize",
        "//enterprise/server/util/cpuset",
        "//proto:oci_fetcher_go_proto",
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
        "//server/util/expflag",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/shlex",
        "//server/util/status",
        "//server/util/statusz",
        "//server/util/tracing",
        "//server/util/usageutil",
        "//server/util/xds",
        "//server/version",
        "//server/xcode",
        "@com_github_google_uuid//:uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//encoding/gzip",
        "@org_golang_google_grpc//xds",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/oci/ociconv",
            "//enterprise/server/remote_execution/cgroup",
            "//enterprise/server/remote_execution/containers/ociruntime",
            "//enterprise/server/remote_execution/vbd",
            "//server/util/networking",
        ],
        "//conditions:default": [],
    }),
)

BINARY_NAME = "executor"

filegroup(
    name = "licenses",
    data = [
        "//enterprise:licenses",
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
        "--config_file=enterprise/config/executor.local.yaml",
        "--port=8888",
        "--monitoring_port=9091",
        "--max_shutdown_duration=3s",
    ],
    data = [
        ":config_files",
        ":licenses",
    ],
    embed = [":executor_lib"],
)

go_test(
    name = "executor_test",
    srcs = ["executor_linux_test.go"],
    embed = [":executor_lib"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)

linux_x86_64_alias(
    name = "executor_linux_amd64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_arm64_alias(
    name = "executor_linux_arm64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

macos_arm64_alias(
    name = "executor_macos_arm64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

macos_x86_64_alias(
    name = "executor_macos_x86_64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_x86_64_musl_alias(
    name = "executor_linux_amd64_static",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_arm64_musl_alias(
    name = "executor_linux_arm64_static",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

windows_amd64_alias(
    name = "executor_windows_amd64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

windows_386_alias(
    name = "executor_windows_386",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

windows_arm64_alias(
    name = "executor_windows_arm64",
    actual = ":executor",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

alias(
    name = "tini",
    actual = select({
        "@platforms//cpu:arm64": "@com_github_krallin_tini_tini-linux-arm64//file:tini",
        "//conditions:default": "@com_github_krallin_tini_tini-linux-amd64//file:tini",
    }),
)

container_layer(
    name = "tini_layer",
    directory = "/",
    files = [":tini"],
    tags = ["manual"],
)

container_layer(
    name = "docker_credential_gcr_layer",
    directory = "/usr/bin",
    files = ["//enterprise/server/cmd/executor/docker_credential_gcr_config:docker-credential-gcr-optional"] + select({
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
        "//conditions:default": ["@com_github_firecracker_microvm_firecracker_amd64//:firecracker"],
    }),
    outs = ["firecracker"],
    cmd = "cp $(SRCS) $@",
)

genrule(
    name = "jailer_rename",
    srcs = select({
        "@platforms//cpu:arm64": ["@com_github_firecracker_microvm_firecracker_arm64//:jailer"],
        "//conditions:default": ["@com_github_firecracker_microvm_firecracker_amd64//:jailer"],
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
    architecture = select({
        "@platforms//cpu:arm64": "arm64",
        "//conditions:default": "amd64",
    }),
    base = "@executor_image//image:dockerfile_image.tar",
    layers = [
        ":tini_layer",
        ":docker_credential_gcr_layer",
        ":docker_credential_gcr_docker_config_layer",
        ":containerd_layer",
        ":runc_layer",
        # Updating these layers does not currently afford us any vulnerability
        # fixes; leave them out for now.
        # ":rootlesskit_layer",
        ":executor_tools_layer",
        ":podman_static_layer",
    ],
    symlinks = {
        "config.yaml": "app/enterprise/server/cmd/executor/executor.runfiles/%s/enterprise/config/executor.release.yaml" % runfiles_directory(),
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
    architecture = select({
        "@platforms//cpu:arm64": "arm64",
        "//conditions:default": "amd64",
    }),
    base = ":base_image",
    binary = ":executor",
    tags = ["manual"],
)

container_image(
    name = "executor_image",
    architecture = select({
        "@platforms//cpu:arm64": "arm64",
        "//conditions:default": "amd64",
    }),
    base = ":executor_go_image",
    entrypoint = [
        "/tini",
        "--",
        "/app/enterprise/server/cmd/executor/executor",
    ],
    tags = ["manual"],
)

image_layer(
    name = "binary_image_layer",
    srcs = {EXECUTOR_PACKAGE_ROOT + "/" + BINARY_NAME: ":" + BINARY_NAME},
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Executor",
        "org.opencontainers.image.description": (
            "Contains the enterprise BuildBuddy executor binary."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    # Do not include runfiles so that we can separate configuration files out
    # into a separate layer. This lets us use a base image without those files
    # when building our production images.
    include_runfiles = False,
    tags = ["manual"],
)

[
    flatten(
        name = "apt_tar_%s" % arch,
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
        tars = [
            # debian packages
            "@debian13//tini/%s" % arch,
            "@debian13//tar/%s" % arch,
            "@debian13//dash/%s" % arch,
            "@debian13//e2fsprogs/%s" % arch,
            "@debian13//fuse3/%s" % arch,
            "@debian13//iproute2/%s" % arch,
            "@debian13//conntrack/%s" % arch,
            "@debian13//amazon-ecr-credential-helper/%s" % arch,
            "@debian13//lxcfs/%s" % arch,

            # nvidia packages
            "@debian13//nvidia-container-toolkit-base/%s" % arch,

            # docker packages
            "@debian13//docker-ce/%s" % arch,
            "@debian13//docker-ce-cli/%s" % arch,
            "@debian13//docker-ce-rootless-extras/%s" % arch,
            "@debian13//containerd.io/%s" % arch,
            "@debian13//docker-buildx-plugin/%s" % arch,
            "@debian13//docker-compose-plugin/%s" % arch,
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

[
    layer_from_tar(
        name = "apt_image_layer_%s" % arch,
        src = "apt_tar_%s" % arch,
        annotations = {
            "org.opencontainers.image.title": "Distroless executor deps (debian13)",
            "org.opencontainers.image.description": (
                "debian13 environment with executor dependencies installed"
            ),
            "org.opencontainers.image.vendor": "BuildBuddy",
        },
        optimize = True,
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

image_layer(
    name = "alternatives_symlink_layer",
    annotations = {
        "org.opencontainers.image.title": "Alternatives symlink layer",
        "org.opencontainers.image.description": (
            "Registers symlinks similarly to `update-alternatives`."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    symlinks = {
        "/usr/bin/sh": "/usr/bin/dash",
        "/usr/sbin/iptables": "/usr/sbin/iptables-nft",
        "/usr/sbin/iptables-restore": "/usr/sbin/iptables-nft-restore",
        "/usr/sbin/iptables-save": "/usr/sbin/iptables-nft-save",
    },
    tags = ["manual"],
)

[
    image_layer(
        name = "docker_credential_gcr_image_layer_%s" % arch,
        srcs = {
            "/usr/bin/docker-credential-gcr": "@com_github_googlecloudplatform_docker-credential-gcr-linux-%s//:docker-credential-gcr.bin" % arch,
            "/usr/bin/docker-credential-gcr-optional": "//enterprise/server/cmd/executor/docker_credential_gcr_config:docker-credential-gcr-optional",
            "/root/.docker/config.json": "//enterprise/server/cmd/executor/docker_credential_gcr_config:config.json",
        },
        annotations = {
            "org.opencontainers.image.title": "docker-credential-gcr",
            "org.opencontainers.image.description": (
                "docker-credential-gcr binary and associated configuration."
            ),
            "org.opencontainers.image.vendor": "BuildBuddy",
        },
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

[
    image_layer(
        name = "containerd_patch_image_layer_%s" % arch,
        srcs = {
            "/usr/bin/containerd": "@com_github_containerd_containerd-linux-%s//:containerd.bin" % arch,
            "/usr/bin/containerd-shim-runc-v2": "@com_github_containerd_containerd-linux-%s//:containerd-shim-runc-v2.bin" % arch,
            "/usr/bin/ctr": "@com_github_containerd_containerd-linux-%s//:ctr.bin" % arch,
        },
        annotations = {
            "org.opencontainers.image.title": "Patch for containerd",
            "org.opencontainers.image.description": (
                "Files to patch debian's standard containerd to remove known vulnerabilities in the go binaries."
            ),
            "org.opencontainers.image.vendor": "BuildBuddy",
        },
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

[
    image_layer(
        name = "runc_patch_image_layer_%s" % arch,
        srcs = {
            "/usr/bin/runc": "@com_github_opencontainers_runc_runc-linux-%s//file:runc" % arch,
        },
        annotations = {
            "org.opencontainers.image.title": "Patch for runc",
            "org.opencontainers.image.description": (
                "Files to patch debian's standard runc to remove known vulnerabilities in the go binaries."
            ),
            "org.opencontainers.image.vendor": "BuildBuddy",
        },
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

[
    image_layer(
        name = "executor_tools_image_layer_%s" % arch,
        srcs = {
            "/usr/bin/firecracker": "@com_github_firecracker_microvm_firecracker_%s//:firecracker" % arch,
            "/usr/bin/jailer": "@com_github_firecracker_microvm_firecracker_%s//:jailer" % arch,
            "/usr/bin/crun": "@com_github_containers_crun_crun-linux-%s//file:crun" % arch,
        },
        annotations = {
            "org.opencontainers.image.title": "Firecracker",
            "org.opencontainers.image.description": (
                "Firecracker binaries."
            ),
            "org.opencontainers.image.vendor": "BuildBuddy",
        },
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

layer_from_tar(
    name = "podman_static_image_layer",
    src = "//enterprise/server/remote_execution/containers/podman:podman-static.tar.gz",
    annotations = {
        "org.opencontainers.image.title": "Podman",
        "org.opencontainers.image.description": (
            "Statically-built podman."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    optimize = True,
    tags = ["manual"],
)

pkg_tar(
    name = "licenses_tar",
    srcs = [
        ":licenses",
    ],
    include_runfiles = True,
    package_dir = EXECUTOR_PACKAGE_ROOT,
    remap_paths = {
        ".runfiles/": "%s.runfiles/" % BINARY_NAME,
    },
    tags = ["manual"],
)

layer_from_tar(
    name = "licenses_image_layer",
    src = "licenses_tar",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Server Licenses",
        "org.opencontainers.image.description": (
            "Contains the licenses for the Buildbuddy executor."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    optimize = True,
    tags = ["manual"],
)

image_manifest(
    name = "base_image_manifest",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Server (unconfigured)",
        "org.opencontainers.image.description": (
            "Contains the BuildBuddy binary, runfiles, and symlinks " +
            "necessary to run the enterprise BuildBuddy server. Does not " +
            "include configuration files."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    base = "//images/distroless/base-nossl:manifest",
    entrypoint = [
        "/usr/bin/tini",
        "--",
        "/%s/%s" % (
            EXECUTOR_PACKAGE_ROOT,
            BINARY_NAME,
        ),
    ],
    layers = select({
        platform: [
            ":apt_image_layer_%s" % arch,
            ":alternatives_symlink_layer",
            ":docker_credential_gcr_image_layer_%s" % arch,
            ":containerd_patch_image_layer_%s" % arch,
            ":runc_patch_image_layer_%s" % arch,
            ":executor_tools_image_layer_%s" % arch,
            ":podman_static_image_layer",
            ":licenses_image_layer",
            ":binary_image_layer",
        ]
        for platform, arch in {
            "@platforms//cpu:x86_64": "amd64",
            "@platforms//cpu:arm64": "arm64",
        }.items()
    }),
    tags = ["manual"],
)

pkg_tar(
    name = "config_files_tar",
    srcs = [
        ":config_files",
    ],
    include_runfiles = True,
    package_dir = EXECUTOR_PACKAGE_ROOT,
    remap_paths = {
        ".runfiles/%s/enterprise" % runfiles_directory(): "",
    },
    tags = ["manual"],
)

pkg_tar(
    name = "config_symlink_tar",
    mode = "0755",
    symlinks = {
        "config.yaml": "%s/config/executor.release.yaml" % EXECUTOR_PACKAGE_ROOT,
    },
    tags = ["manual"],
)

flatten(
    name = "config_tar",
    tags = ["manual"],
    tars = [
        ":config_files_tar",
        ":config_symlink_tar",
    ],
)

layer_from_tar(
    name = "config_layer",
    src = "config_tar",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Executor Configuration (FOSS)",
        "org.opencontainers.image.description": (
            "Contains the FOSS enterprise BuildBuddy executor configuration files."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    optimize = True,
    tags = ["manual"],
)

image_manifest(
    name = "image_manifest",
    annotations = {
        "org.opencontainers.image.title": "Enterprise BuildBuddy Executor",
        "org.opencontainers.image.description": (
            "Contains the BuildBuddy executor binary, runfiles, configuration " +
            "files, and symlinks necessary to run the enterprise BuildBuddy " +
            "executor."
        ),
        "org.opencontainers.image.vendor": "BuildBuddy",
    },
    base = ":base_image_manifest",
    layers = [
        ":config_layer",
    ],
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

image_index(
    name = "index",
    manifests = [":image_manifest"],
    platforms = [
        "//platforms:linux_x86_64",
        "//platforms:linux_arm64",
    ],
    tags = ["manual"],
)

image_load(
    name = "image",
    # Explicitly use containerd for clarity. The container will be accessible
    # via `nerdctl` in the "moby" namespace.
    #
    # We use containerd, as advised by `rules_img`, to avoid `docker load`,
    # which, if the host system is not using the containerd backend for docker,
    # will create a custom docker manifest.
    # Loading into containerd allows us to load the exact OCI image we've built
    # with the expected digest.
    daemon = "containerd",
    image = ":index",
    registry = "buildbuddy.io",
    repository = "enterprise/executor",
    tag = "latest",
    tags = ["manual"],
)

# This image is the docker image used by the executor to run actions that do not
# already have a docker image specified.
container_image(
    name = "default_base_image",
    base = "@default_execution_image//image:dockerfile_image.tar",
    tags = ["manual"],
)
