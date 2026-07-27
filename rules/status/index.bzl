def _stamp_detected_inner_impl(ctx):
  if ctx.attr.stamp == 1 or (ctx.attr.stamp == -1 and ctx.attr.private_stamp_detect):
      # stamped path
      args.append("--stamp_from=%s" % ctx.version_file.path)
      files.append(ctx.version_file)
  else:
      # unstamped path.

stamp_detected_inner = rule(
    implementation = _stamp_detected_inner_impl,
    attrs = {
        "stamp": attr.int(
            default = 0,  # Mimic the *_binary stamp behavior.
        ),
        # Is --stamp set on the command line?
        "private_stamp_detect": attr.bool(default = False),
    },
)

def stamp_detected(ctx, name, **kwargs):
    stamp_detected_inner(
        name = name,
        private_stamp_detect = select({
            "//rules/status:private_stamp_detect": True,
            "//conditions:default": False,
        }),
        **kwargs,
    )

def _stamp_vars(ctx):
    if stamp_detected(ctx, ctx.label.name + ".stamp_detected") 
    return [
        platform_common.TemplateVariableInfo({
            for key in ctx.attr.keys
        }),
    ]

stamp_vars = rule(
    implementation = _stamp_vars,
    attrs = {keys: attr.string_list()},
)
