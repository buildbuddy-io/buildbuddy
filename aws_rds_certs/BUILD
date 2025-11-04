load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //visibility:public
package(default_visibility = ["//visibility:public"])

# Certs that are distributed with the server binary.
filegroup(
    name = "embedded_certs",
    srcs = [":rds-combined-ca-bundle.pem"],
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":embedded_certs"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/aws_rds_certs",
    deps = [
        "//server/util/fileresolver",
    ],
)
