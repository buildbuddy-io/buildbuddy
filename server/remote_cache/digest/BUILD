load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:ignore

go_library(
    name = "digest",
    srcs = ["digest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/util/alert",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@com_github_zeebo_blake3//:blake3",
        "@org_golang_google_genproto_googleapis_rpc//errdetails",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "digest_test",
    size = "small",
    srcs = [
        "digest_bench_test.go",
        "digest_test.go",
    ],
    embed = [":digest"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/util/compression",
        "//server/util/status",
        "@com_github_klauspost_cpuid_v2//:cpuid",
        "@com_github_minio_sha256_simd//:sha256-simd",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "simd_bench_test",
    srcs = ["digest_bench_test.go"],
    args = [
        "-test.bench=.",
        "-test.benchtime=10000x",
    ],
    embed = [":digest"],
    tags = [
        "manual",
        "no-remote-cache",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/util/compression",
        "//server/util/status",
        "@com_github_klauspost_cpuid_v2//:cpuid",
        "@com_github_minio_sha256_simd//:sha256-simd",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//status",
    ],
)
