load("@aspect_rules_js//js:defs.bzl", "js_library")
load("@com_github_buildbuddy_io_protoc_gen_protobufjs//:rules.bzl", "protoc_gen_protobufjs")
load("@io_bazel_rules_go//proto:def.bzl", _go_proto_library = "go_proto_library")

def go_proto_library(name, compilers = [], **kwargs):
    """Wrapper for go_proto_library that ensures vtprotobuf support.

    This wrapper automatically includes the required compilers:
    - @io_bazel_rules_go//proto:go_proto
    - //proto:vtprotobuf_compiler

    Any additional compilers specified (e.g., go_grpc_v2) are added on top.

    Args:
        name: name of the go_proto_library target
        compilers: list of additional compilers to include (e.g., ["@io_bazel_rules_go//proto:go_grpc_v2"])
        **kwargs: passed through to go_proto_library
    """
    default_compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "//proto:vtprotobuf_compiler",
    ]

    # Combine default compilers with any additional ones
    all_compilers = default_compilers + compilers

    _go_proto_library(
        name = name,
        compilers = all_compilers,
        **kwargs
    )

def ts_proto_library(name, proto, deps = [], **kwargs):
    """Generates .js and .d.ts files from a proto_library target.

    Args:
        name: name of generated js_library target, also used to name the .js/.d.ts output
        proto: label of a single proto_library target to generate code for
        deps: deps for *directly* imported protos only; must be other ts_proto_library targets
        **kwargs: passed through to the underlying rules
    """

    protoc_gen_protobufjs(
        name = name + "__gen_protobufjs",
        out = name,
        proto = proto,
        deps = [dep + "__gen_protobufjs" for dep in deps],
        **kwargs
    )

    js_library(
        name = name,
        srcs = [":" + name + "__gen_protobufjs"],
        deps = [
            "//:node_modules/@types/long",
            "//:node_modules/long",
            "//:node_modules/protobufjs",
            "//:node_modules/tslib",
        ] + deps,
        **kwargs
    )
