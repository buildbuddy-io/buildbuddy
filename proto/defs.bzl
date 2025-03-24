load("@build_bazel_rules_nodejs//:index.bzl", "js_library")
load("@com_github_buildbuddy_io_protoc_gen_protobufjs//:rules.bzl", "protoc_gen_protobufjs")

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
            "@npm//@types/long",
            "@npm//long",
            "@npm//protobufjs",
            "@npm//tslib",
        ] + deps,
        **kwargs
    )
