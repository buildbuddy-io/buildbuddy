load("@io_bazel_rules_go//go:def.bzl", "GoSource", "go_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

# Defines a go_proto_library for a service, along with a "_protolet"
# target that contains generated protolet handlers.
def go_protolet_stream_library(name, proto, deps, importpath, compilers = None, **kwargs):
    go_proto_library(
        name = name,
        deps = deps,
        importpath = importpath,
        proto = proto,
        compilers = compilers,
        **kwargs
    )

    # Work around go_proto_library not exposing a normal file provider for the
    # generated .pb.go file.
    _copy_go_source(
        name = "%s_pb_source" % name,
        src = ":%s" % name,
        out = "%s_pb_source.pb.go" % name,
    )

    native.genrule(
        name = "%s_protolet_src" % name,
        srcs = [":%s_pb_source" % name],
        tools = ["//rules/protolet:generate_stream_api.py"],
        outs = ["%s.protolet.go" % name],
        cmd_bash = """
        SERVICE_PACKAGE="%s" python3 $(location //rules/protolet:generate_stream_api.py) $(SRCS) > $@
        """ % (importpath),
    )

    go_library(
        name = "%s_protolet" % name,
        importpath = importpath + "_protolet",
        srcs = ["%s_protolet_src" % name],
        deps = deps + [
            ":%s" % name,
            "//server/http/protolet",
            "//server/util/status",
            "@org_golang_google_grpc//:grpc",
            "@org_golang_google_protobuf//proto",
        ],
        **kwargs
    )

def _copy_go_source_impl(ctx):
    src_file = ctx.attr.src[GoSource].srcs[0]
    dst_file = ctx.actions.declare_file(ctx.attr.out)
    ctx.actions.run_shell(
        inputs = [src_file],
        outputs = [dst_file],
        command = "cp %s %s" % (src_file.path, dst_file.path),
    )
    return [DefaultInfo(files = depset([dst_file]))]

_copy_go_source = rule(
    attrs = {
        "src": attr.label(mandatory = True, providers = [GoSource]),
        "out": attr.string(mandatory = True),
    },
    implementation = _copy_go_source_impl,
)
