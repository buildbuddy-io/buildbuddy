load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "protoc-gen-go-vtproto-view_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/protoc-gen-go-vtproto-view",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:view_go_proto",
        "@com_github_planetscale_vtprotobuf//generator",
        "@org_golang_google_protobuf//compiler/protogen",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)

go_binary(
    name = "protoc-gen-go-vtproto-view",
    embed = [":protoc-gen-go-vtproto-view_lib"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "protoc-gen-go-vtproto-view_test",
    srcs = ["golden_test.go"],
    data = [
        "//tools/protoc-gen-go-vtproto-view/testdata:hello_vtview.pb.go.golden",
        "//tools/protoc-gen-go-vtproto-view/testdata:viewtest_vtview.pb.go.golden",
    ],
    embed = [":protoc-gen-go-vtproto-view_lib"],
    deps = [
        "//tools/protoc-gen-go-vtproto-view/testdata:hello_go_proto",
        "//tools/protoc-gen-go-vtproto-view/testdata:viewtest_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//compiler/protogen",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protodesc",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//reflect/protoregistry",
        "@org_golang_google_protobuf//types/descriptorpb",
        "@org_golang_google_protobuf//types/pluginpb",
    ],
)
