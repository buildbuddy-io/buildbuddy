load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "buildbuddy_server",
    srcs = ["buildbuddy_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server",
    deps = [
        ":environment",
        ":interfaces",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/build_event_protocol:build_event_handler",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/config",
    deps = [
        "@in_gopkg_yaml_v2//:go_default_library",
    ],
)

go_library(
    name = "environment",
    srcs = ["environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/environment",
    deps = [
        ":config",
        ":interfaces",
        "//server/backends:blobstore",
        "//server/backends:cache",
        "//server/backends:database",
        "//server/backends:simplesearcher",
        "//server/backends:slack",
        "//server/build_event_protocol:build_event_proxy",
        "//server/util:healthcheck",
    ],
)

go_library(
    name = "janitor",
    srcs = ["janitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/janitor",
    deps = [
        ":config",
        ":environment",
        ":interfaces",
        ":tables",
        "//server/backends:database",
    ],
)

go_library(
    name = "interfaces",
    srcs = ["interfaces.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/interfaces",
    deps = [
        ":tables",
        "//proto:invocation_go_proto",
    ],
)

go_library(
    name = "static",
    srcs = ["static.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/static",
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

go_library(
    name = "tables",
    srcs = [
        "tables.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/tables",
    deps = [
        "//proto:invocation_go_proto",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)

go_library(
    name = "libmain",
    srcs = [
        "libmain.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/libmain",
    deps = [
        ":buildbuddy_server",
        ":environment",
        ":static",
        "//proto:buildbuddy_service_go_proto",
        "//proto:publish_build_event_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/build_event_protocol:build_event_server",
        "//server/http:filters",
        "//server/http:protolet",
        "//server/remote_cache:action_cache_server",
        "//server/remote_cache:byte_stream_server",
        "//server/remote_cache:capabilities_server",
        "//server/remote_cache:content_addressable_storage_server",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//reflection:go_default_library",
    ],
)

# Use the go_binary rule to create an executable from our main file. Depend on
# the static_content we created above so they are included.
go_binary(
    name = "buildbuddy",
    srcs = ["main.go"],
    data = [
        "//:config_files",
        "//app:app_bundle.min",
        "//static",
    ],
    deps = [
        ":config",
        ":environment",
        ":janitor",
        ":libmain",
        "//server/util:healthcheck",
    ],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "buildbuddy_image",
    binary = ":buildbuddy",
)
