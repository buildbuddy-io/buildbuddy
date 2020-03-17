load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "blobstore",
    srcs = ["blobstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/blobstore",
    deps = [
        ":config",
        ":interfaces",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "build_event_handler",
    srcs = ["build_event_handler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_handler",
    deps = [
        ":config",
        ":environment",
        ":event_parser",
        ":interfaces",
        ":protofile",
        ":tables",
        "//proto:build_event_stream_go_proto",
        "//proto:build_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
    ],
)

go_library(
    name = "build_event_proxy",
    srcs = ["build_event_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_proxy",
    deps = [
        "//proto:build_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_library(
    name = "build_event_server",
    srcs = ["build_event_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_server",
    deps = [
        ":build_event_handler",
        ":environment",
        "//proto:build_event_stream_go_proto",
        "//proto:build_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@io_bazel_rules_go//proto/wkt:empty_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_library(
    name = "buildbuddy_server",
    srcs = ["buildbuddy_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server",
    deps = [
        ":build_event_handler",
        ":environment",
        ":interfaces",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
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
    name = "database",
    srcs = [
        "database.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/database",
    deps = [
        ":config",
        ":tables",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@com_github_jinzhu_gorm//dialects/mysql:go_default_library",
        "@com_github_jinzhu_gorm//dialects/postgres:go_default_library",
        "@com_github_jinzhu_gorm//dialects/sqlite:go_default_library",
    ],
)

go_library(
    name = "environment",
    srcs = ["environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/environment",
    deps = [
        ":blobstore",
        ":build_event_proxy",
        ":config",
        ":database",
        ":healthcheck",
        ":interfaces",
        ":simplesearcher",
        ":slack",
    ],
)

go_library(
    name = "event_parser",
    srcs = [
        "event_parser.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/event_parser",
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
    ],
)

go_library(
    name = "healthcheck",
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/healthcheck",
)

go_library(
    name = "httpfilters",
    srcs = ["httpfilters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/httpfilters",
)

go_library(
    name = "janitor",
    srcs = ["janitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/janitor",
    deps = [
        ":config",
        ":database",
        ":environment",
        ":interfaces",
        ":tables",
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
    name = "protofile",
    srcs = ["protofile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/protofile",
    deps = [
        ":interfaces",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "protolet",
    srcs = ["protolet.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/protolet",
    deps = [
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "simplesearcher",
    srcs = ["simplesearcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/simplesearcher",
    deps = [
        ":database",
        ":tables",
        "//proto:invocation_go_proto",
    ],
)

go_library(
    name = "slack",
    srcs = ["slack.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/slack",
    deps = [
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
        ":build_event_server",
        ":buildbuddy_server",
        ":environment",
        ":httpfilters",
        ":protolet",
        ":static",
        "//proto:build_go_proto",
        "//proto:buildbuddy_service_go_proto",
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
        ":healthcheck",
        ":janitor",
        ":libmain",
    ],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "buildbuddy_image",
    binary = ":buildbuddy",
)
