def get_parent_from_constraints(constraints):
    if "@platforms//os:linux" in constraints:
        if "@platforms//cpu:x86_64" in constraints:
            return ":linux_x86_64"
        return ":linux"
    if "@platforms//os:osx" in constraints:
        return ":macos"
    return "@local_config_platform//:host"
