load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//tools/protoc-gen-go-vtproto-view:__subpackages__"])

exports_files([
    "hello_vtview.pb.go.golden",
    "viewtest_vtview.pb.go.golden",
])

proto_library(
    name = "hello_proto",
    srcs = ["hello.proto"],
    deps = ["//proto:view_proto"],
)

go_proto_library(
    name = "hello_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/protoc-gen-go-vtproto-view/testdata/hello",
    proto = ":hello_proto",
    deps = ["//proto:view_go_proto"],
)

proto_library(
    name = "viewtest_proto",
    srcs = ["viewtest.proto"],
    deps = ["//proto:view_proto"],
)

# Deliberately a plain go_proto_library (no vtprotobuf compilers): the golden
# test links vtprotobuf's generator package, whose copy of the vtproto
# extension package would conflict with the proto-generated copy that the
# vtprotobuf compilers pull in.
go_proto_library(
    name = "viewtest_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/protoc-gen-go-vtproto-view/testdata/viewtest",
    proto = ":viewtest_proto",
    deps = ["//proto:view_go_proto"],
)
