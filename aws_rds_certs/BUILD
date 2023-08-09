load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //visibility:public
package(default_visibility = ["//visibility:public"])

# Synthesize a copy of the file in the current package so it can be embedded.
genrule(
    name = "aws_rds_certs",
    srcs = ["@aws_rds_certs//file:rds-combined-ca-bundle.pem"],
    outs = ["rds-combined-ca-bundle.pem"],
    cmd_bash = "cp $(SRCS) $@",
)

# Certs that are distributed with the server binary.
filegroup(
    name = "embedded_certs",
    srcs = [":rds-combined-ca-bundle.pem"],
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":embedded_certs"],
    importpath = "github.com/buildbuddy-io/buildbuddy/aws_rds_certs",
    deps = [
        "//server/util/fileresolver",
    ],
)
