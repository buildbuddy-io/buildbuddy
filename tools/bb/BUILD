load("@rules_python//python:defs.bzl", "py_binary")

# Prebuilt bb CLI binary, used by //tools/lint.
# This avoids building the full bb CLI from source (which pulls in
# Kubernetes client-go, gRPC xDS, etc.) just to shell out to `bb fix`.
#
# To update the version:
#   bazel run //tools/bb:update_version -- <version>
# or run the script directly:
#   python3 tools/bb/update_version.py <version>
# where <version> is a tag from https://github.com/buildbuddy-io/bazel/releases
alias(
    name = "bb",
    actual = select({
        "//platforms/configs:macos_arm64": "@io_buildbuddy_bb_cli-darwin-arm64//file:downloaded",
        "//platforms/configs:macos_x86_64": "@io_buildbuddy_bb_cli-darwin-x86_64//file:downloaded",
        "//platforms/configs:linux_arm64": "@io_buildbuddy_bb_cli-linux-arm64//file:downloaded",
        "//platforms/configs:linux_x86_64": "@io_buildbuddy_bb_cli-linux-x86_64//file:downloaded",
    }),
    visibility = ["//tools/lint:__pkg__"],
)

py_binary(
    name = "update_version",
    srcs = ["update_version.py"],
    main = "update_version.py",
)
