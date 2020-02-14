# Copyright 2017 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"Protocol Buffers"

load("@build_bazel_rules_nodejs//:providers.bzl", "DeclarationInfo", "JSEcmaScriptModuleInfo", "JSNamedModuleInfo")

def _run_pbjs(actions, executable, var, output_name, proto_files, suffix = ".js", wrap = "commonjs", amd_name = ""):
    js_file = actions.declare_file(output_name + suffix)

    # Create an intermediate file so that we can do some manipulation of the
    # generated .js output that makes it compatible with our named AMD loading.
    js_tmpl_file = actions.declare_file(output_name + suffix + ".tmpl")

    # Reference of arguments:
    # https://github.com/dcodeIO/ProtoBuf.js/#pbjs-for-javascript
    args = actions.args()
    args.add_all(["--target", "static-module"])
    args.add_all(["--wrap", wrap])
    args.add("--strict-long")  # Force usage of Long type with int64 fields
    args.add_all(["--out", js_file.path + ".tmpl"])
    args.add_all(proto_files)

    actions.run(
        executable = executable._pbjs,
        inputs = proto_files,
        outputs = [js_tmpl_file],
        arguments = [args],
        env = {"COMPILATION_MODE": var["COMPILATION_MODE"]},
    )

    actions.expand_template(
        template = js_tmpl_file,
        output = js_file,
        substitutions = {
            # convert anonymous AMD module
            #  define(["protobufjs/minimal"], function($protobuf) {
            # to named
            #  define("wksp/path/to/module", ["protobufjs/minimal"], ...
            "define([": "define('%s/%s', [" % (amd_name, output_name),
        },
    )
    return js_file

def _run_pbts(actions, executable, var, js_file):
    ts_file = actions.declare_file(js_file.basename[:-len(".js")] + ".d.ts")

    # Reference of arguments:
    # https://github.com/dcodeIO/ProtoBuf.js/#pbts-for-typescript
    args = actions.args()
    args.add_all(["--out", ts_file.path])
    args.add(js_file.path)

    actions.run(
        executable = executable._pbts,
        progress_message = "Generating typings from %s" % js_file.short_path,
        inputs = [js_file],
        outputs = [ts_file],
        arguments = [args],
        env = {"COMPILATION_MODE": var["COMPILATION_MODE"]},
    )
    return ts_file

def _ts_proto_library(ctx):
    sources_depsets = []
    for dep in ctx.attr.deps:
        if ProtoInfo not in dep:
            fail("ts_proto_library dep %s must be a proto_library rule" % dep.label)

        # TODO(alexeagle): go/new-proto-library suggests
        # > should not parse .proto files. Instead, they should use the descriptor
        # > set output from proto_library
        # but protobuf.js doesn't seem to accept that bin format
        sources_depsets.append(dep[ProtoInfo].transitive_sources)

    sources = depset(transitive = sources_depsets)

    output_name = ctx.attr.output_name or ctx.label.name

    js_es5 = _run_pbjs(
        ctx.actions,
        ctx.executable,
        ctx.var,
        output_name,
        sources,
        amd_name = "/".join([p for p in [
            ctx.workspace_name,
            ctx.label.package,
        ] if p]),
    )
    js_es6 = _run_pbjs(
        ctx.actions,
        ctx.executable,
        ctx.var,
        output_name,
        sources,
        suffix = ".mjs",
        wrap = "es6",
    )

    # pbts doesn't understand '.mjs' extension so give it the es5 file
    dts = _run_pbts(ctx.actions, ctx.executable, ctx.var, js_es5)

    declarations = depset([dts])
    es5_sources = depset([js_es5])
    es6_sources = depset([js_es6])

    # Return a structure that is compatible with the deps[] of a ts_library.
    return struct(
        providers = [
            DefaultInfo(files = declarations),
            DeclarationInfo(
                declarations = declarations,
                transitive_declarations = declarations,
                type_blacklisted_declarations = depset([]),
            ),
            JSNamedModuleInfo(
                direct_sources = es5_sources,
                sources = es5_sources,
            ),
            JSEcmaScriptModuleInfo(
                direct_sources = es6_sources,
                sources = es6_sources,
            ),
        ],
        typescript = struct(
            declarations = declarations,
            transitive_declarations = declarations,
            type_blacklisted_declarations = depset(),
            es5_sources = es5_sources,
            es6_sources = es6_sources,
            transitive_es5_sources = es5_sources,
            transitive_es6_sources = es6_sources,
        ),
    )


ts_proto_library = rule(
    implementation = _ts_proto_library,
    attrs = {
        "output_name": attr.string(
            doc = """Name of the resulting module, which you will import from.
            If not specified, the name will match the target's name.""",
        ),
        "deps": attr.label_list(doc = "proto_library targets"),
        "_pbjs": attr.label(
            default = Label("//rules:pbjs"),
            executable = True,
            cfg = "host",
        ),
        "_pbts": attr.label(
            default = Label("//rules:pbts"),
            executable = True,
            cfg = "host",
        ),
    },
    doc = """""",
)
