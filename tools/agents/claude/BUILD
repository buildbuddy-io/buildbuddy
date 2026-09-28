load("//rules/typescript:index.bzl", "ts_embed", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_embed(
    name = "setup_ts",
    src = "setup.sh",
    out = "setup.ts",
)

ts_library(
    name = "setup",
    srcs = ["setup.ts"],
)
