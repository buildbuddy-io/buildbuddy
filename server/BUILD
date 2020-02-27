load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "blobstore",
    srcs = ["blobstore.go"],
    importpath = "github.com/tryflame/buildbuddy/server/blobstore",
    deps = [
        ":config",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "build_event_handler",
    srcs = ["build_event_handler.go"],
    importpath = "github.com/tryflame/buildbuddy/server/build_event_handler",
    deps = [
        ":blobstore",
        ":database",
        ":event_parser",
        ":tables",
        "//proto:build_event_stream_go_proto",
        "//proto:build_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)

go_library(
    name = "build_event_server",
    srcs = ["build_event_server.go"],
    importpath = "github.com/tryflame/buildbuddy/server/build_event_server",
    deps = [
        ":build_event_handler",
        "//proto:build_event_stream_go_proto",
        "//proto:build_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@io_bazel_rules_go//proto/wkt:empty_go_proto",
    ],
)

go_library(
    name = "buildbuddy_server",
    srcs = ["buildbuddy_server.go"],
    importpath = "github.com/tryflame/buildbuddy/server/buildbuddy_server",
    deps = [
        ":build_event_handler",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/tryflame/buildbuddy/server/config",
    deps = [
        "@in_gopkg_yaml_v2//:go_default_library",
    ],
)

go_library(
    name = "database",
    srcs = [
        "database.go",
    ],
    importpath = "github.com/tryflame/buildbuddy/server/database",
    deps = [
        ":config",
        ":tables",
        "//proto:invocation_go_proto",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@com_github_jinzhu_gorm//dialects/mysql:go_default_library",
        "@com_github_jinzhu_gorm//dialects/postgres:go_default_library",
        "@com_github_jinzhu_gorm//dialects/sqlite:go_default_library",
    ],
)

go_library(
    name = "event_parser",
    srcs = [
        "event_parser.go",
    ],
    importpath = "github.com/tryflame/buildbuddy/server/event_parser",
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
    ],
)

go_library(
    name = "janitor",
    srcs = ["janitor.go"],
    importpath = "github.com/tryflame/buildbuddy/server/janitor",
    deps = [
        ":blobstore",
        ":config",
        ":database",
        ":tables",
    ],
)

go_library(
    name = "protolet",
    srcs = ["protolet.go"],
    importpath = "github.com/tryflame/buildbuddy/server/protolet",
    deps = [
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "static",
    srcs = ["static.go"],
    importpath = "github.com/tryflame/buildbuddy/server/static",
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

go_library(
    name = "tables",
    srcs = [
        "tables.go",
    ],
    importpath = "github.com/tryflame/buildbuddy/server/tables",
    deps = [
        "//proto:invocation_go_proto",
        "@com_github_jinzhu_gorm//:go_default_library",
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
        ":blobstore",
        ":build_event_handler",
        ":build_event_server",
        ":buildbuddy_server",
        ":config",
        ":database",
        ":janitor",
        ":protolet",
        ":static",
        "//proto:build_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "buildbuddy_image",
    binary = ":buildbuddy",
)
