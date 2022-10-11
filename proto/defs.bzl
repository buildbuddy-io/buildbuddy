load("@aspect_rules_js//js:defs.bzl", "js_library")
load("@npm//:protobufjs-cli/package_json.bzl", "bin")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")

def _proto_sources_impl(ctx):
    transitive_srcs = ctx.attr.proto[ProtoInfo].transitive_sources
    outs = []
    for src in transitive_srcs.to_list():
        out = ctx.actions.declare_file("%s/%s" % (ctx.bin_dir.path, src.short_path))
        ctx.actions.run_shell(
            outputs = [out],
            inputs = [src],
            command = "cp %s %s" % (src.path, out.path),
        )
        outs.append(out)

    return DefaultInfo(files = depset(outs))

_proto_sources = rule(
    doc = """Provider Adapter from ProtoInfo to DefaultInfo.
        Extracts the transitive_sources from the ProtoInfo provided by the proto attr.
        This allows a macro to access the complete set of .proto files needed during compilation.
        """,
    implementation = _proto_sources_impl,
    attrs = {"proto": attr.label(providers = [ProtoInfo])},
)

def ts_proto_library(name, proto, deps = [], **kwargs):
    """Minimal wrapper macro around pbjs/pbts tooling

    Args:
        name: name of generated js_library target, also used to name the .js/.d.ts outputs
        proto: label of a single proto_library target to generate for
        **kwargs: passed through to the js_library
    """

    js_out = name + ".js"
    ts_out = js_out.replace(".js", ".d.ts")

    # Generate some target names, based on the provided name
    # (so that they are unique if the macro is called several times in one package)
    proto_target = "_%s_protos" % name
    js_target = "_%s_pbjs" % name
    ts_target = "_%s_pbts" % name

    # grab the transitive .proto files needed to compile the given one
    _proto_sources(
        name = proto_target,
        proto = proto,
    )

    # Transform .proto files to a single _pb.js file named after the macro
    bin.pbjs(
        name = js_target,
        srcs = [":" + proto_target],
        # We explicitly copy the sources to bin via the _proto_sources rule.
        # This is needed because the copy_srcs_to_bin attribute here doesn't
        # support external repositories.
        copy_srcs_to_bin = False,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbjs-for-javascript
        args = [
            "--force-message",
            "--target=static-module",
            "--wrap=es6",
            "--root=%s" % name,
            "--strict-long",  # Force usage of Long type with int64 fields
            "--out=$(rootpath %s)" % js_out,
            "$(rootpaths %s)" % proto_target,
        ],
        outs = [js_out],
    )

    # Transform the _pb.js file to a .d.ts file with TypeScript types
    bin.pbts(
        name = ts_target,
        srcs = [js_target],
        # env = {"BAZEL_BINDIR": "."},
        # copy_srcs_to_bin = False,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbts-for-typescript
        args = [
            "--out=$(rootpath %s)" % ts_out,
            "$(rootpath %s)" % js_target,
        ],
        outs = [ts_out],
    )

    # Expose the results as js_library which provides DeclarationInfo for interop with other rules
    js_library(
        name = name,
        srcs = [
            js_target,
            ts_target,
        ],
        deps = deps + [
            "//:node_modules/@types/long",
            "//:node_modules/long",
            "//:node_modules/protobufjs",
        ],
        **kwargs
    )
