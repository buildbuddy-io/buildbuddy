load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")

def _write_flag_to_file_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.write(out, ctx.attr.template % ctx.attr.flag[BuildSettingInfo].value)
    return DefaultInfo(files = depset([out]))

write_flag_to_file = rule(
    doc = "Writes a starlark flag's value to a file.",
    implementation = _write_flag_to_file_impl,
    attrs = {
        "flag": attr.label(providers = [BuildSettingInfo]),
        "template": attr.string(default = "%s", doc = "A string where the literal '%s' is substituted with the flag value."),
    },
)
