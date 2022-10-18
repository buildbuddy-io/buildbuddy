load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")

def _write_version_src_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.out)
    ctx.actions.write(out, """package version

var cliVersionFlag = "%s"
""" % ctx.attr.flag[BuildSettingInfo].value)
    return DefaultInfo(files = depset([out]))

write_version_src = rule(
    implementation = _write_version_src_impl,
    attrs = {
        "out": attr.string(mandatory = True),
        "flag": attr.label(providers = [BuildSettingInfo]),
    },
)
