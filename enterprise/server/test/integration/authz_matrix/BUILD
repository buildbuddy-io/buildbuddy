load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Black-box authorization matrix test. Runs against a live BuildBuddy
# deployment; see authz_matrix_test.go for the flags.
#
# Example:
#   bazel test //enterprise/server/test/integration/authz_matrix \
#     --test_output=streamed \
#     --test_env=AUTHZ_MATRIX_API_KEY=... \
#     --test_arg=--target=grpcs://buildbuddy-qa-dev.buildbuddy.dev
go_test(
    name = "authz_matrix_test",
    srcs = ["authz_matrix_test.go"],
    exec_properties = {
        "test.dockerNetwork": "bridge",
    },
    tags = [
        "manual",
        "requires-network",
    ],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:publish_build_event_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:user_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/util/grpc_client",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//types/dynamicpb",
    ],
)
