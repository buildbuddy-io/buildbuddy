load("@build_bazel_rules_nodejs//:index.bzl", "js_library")

# TODO switch to protobufjs-cli when its published
# https://github.com/protobufjs/protobuf.js/commit/da34f43ccd51ad97017e139f137521782f5ef119
load("@npm//protobufjs:index.bzl", "pbjs", "pbts")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")

# protobuf.js relies on these packages, but does not list them as dependencies
# in its package.json.
# Instead they are listed under "cliDependencies"
# (see https://unpkg.com/protobufjs@6.10.2/package.json)
# When run, the CLI attempts to run `npm install` at runtime to get them.
# This fails under Bazel as it tries to access the npm cache outside of the sandbox.
# Per Bazel semantics, all dependencies should be pre-declared.
# Note, you'll also need to install all of these in your package.json!
# (This should be fixed when we switch to protobufjs-cli)
_PROTOBUFJS_CLI_DEPS = ["@npm//%s" % s for s in [
    "chalk",
    "escodegen",
    "espree",
    "estraverse",
    "glob",
    "jsdoc",
    "minimist",
    "semver",
    "tmp",
    "uglify-js",
]]

def _proto_sources_impl(ctx):
    return DefaultInfo(files = ctx.attr.proto[ProtoInfo].transitive_sources)

_proto_sources = rule(
    doc = """Provider Adapter from ProtoInfo to DefaultInfo.
        Extracts the transitive_sources from the ProtoInfo provided by the proto attr.
        This allows a macro to access the complete set of .proto files needed during compilation.
        """,
    implementation = _proto_sources_impl,
    attrs = {"proto": attr.label(providers = [ProtoInfo])},
)

def ts_proto_library(name, proto, **kwargs):
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
    pbjs(
        name = js_target,
        data = [":" + proto_target] + _PROTOBUFJS_CLI_DEPS,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbjs-for-javascript
        args = [
            "--target=static-module",
            "--wrap=default",
            "--strict-long",  # Force usage of Long type with int64 fields
            "--out=$@",
            "$(execpaths %s)" % proto_target,
        ],
        outs = [js_out],
    )

    # Transform the _pb.js file to a .d.ts file with TypeScript types
    pbts(
        name = ts_target,
        data = [js_target] + _PROTOBUFJS_CLI_DEPS,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbts-for-typescript
        args = [
            "--out=$@",
            "$(execpath %s)" % js_target,
        ],
        outs = [ts_out],
    )

    # umd_bundle(
    #     name = name + "__umd",
    #     package_name = name,
    #     entry_point = ":" + js_out,
    # )

    # Expose the results as js_library which provides DeclarationInfo for interop with other rules
    js_library(
        name = name,
        srcs = [
            js_target,
            ts_target,
        ],
        **kwargs
    )

def _umd_bundle(ctx):
    if len(ctx.attr.entry_point.files.to_list()) != 1:
        fail("labels in entry_point must contain exactly one file")

    output = ctx.actions.declare_file("%s.umd.js" % ctx.attr.package_name)

    args = ctx.actions.args()
    args.add(ctx.workspace_name)
    args.add(ctx.attr.package_name)
    args.add(ctx.file.entry_point.path)
    args.add(output.path)

    ctx.actions.run(
        progress_message = "Generated UMD bundle for %s npm package [browserify]" % ctx.attr.package_name,
        executable = ctx.executable._browserify_wrapped,
        inputs = ctx.attr.srcs,
        outputs = [output],
        arguments = [args],
    )

    return [
        DefaultInfo(files = depset([output]), runfiles = ctx.runfiles([output])),
        OutputGroupInfo(umd = depset([output])),
    ]

umd_bundle = rule(
    implementation = _umd_bundle,
    attrs = {
        "srcs": attr.label_list(
            doc = "Extra sources to include in the bundle.",
        ),
        "package_name": attr.string(
            doc = "Generated UMD package name.",
            mandatory = True,
        ),
        "entry_point": attr.label(
            doc = "File containing the top-level module.",
            mandatory = True,
            allow_single_file = True,
        ),
        "_browserify_wrapped": attr.label(
            executable = True,
            cfg = "host",
            default = Label("@build_bazel_rules_nodejs//internal/npm_install:browserify-wrapped"),
        ),
    },
    outputs = {"umd": "%{package_name}.umd.js"},
    doc = """Node package umd bundling""",
)
