load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "build_event_server",
    srcs = ["build_event_server.go"],
    importpath = "github.com/tryflame/buildbuddy/server/build_event_server",
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_go_proto",
        "@com_github_golang_protobuf//jsonpb:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@io_bazel_rules_go//proto/wkt:empty_go_proto",
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

# Use the go_binary rule to create an executable from our main file. Depend on
# the static_content we created above so they are included.
go_binary(
    name = "buildbuddy",
    srcs = ["main.go"],
    data = [
        "//app:app_bundle",
    ],
    deps = [
        ":build_event_server",
        ":static",
        "//proto:build_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "buildbuddy_image",
    srcs = ["main.go"],
    deps = [
        ":static",
    ],
)
