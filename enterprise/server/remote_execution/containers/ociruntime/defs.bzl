load("@io_bazel_rules_go//go:def.bzl", "go_test")

def ociruntime_tests(name, env = {}, exec_properties = {}, **kwargs):
    """Creates a go_test rule for each of the rootless and non-rootless tests.

    This macro mostly exists so that gazelle can update the test rules for us;
    gazelle struggles to handle multiple test rules with
    the same test source file.
    """
    for rootless in [False, True]:
        env = dict(env)
        exec_properties = dict(exec_properties)

        name = name + ("_rootless" if rootless else "")
        if rootless:
            exec_properties["test.dockerUser"] = "1000:1000"
            env["ROOTLESS_TEST"] = "1" if rootless else "0"

        go_test(
            name = name,
            env = env,
            exec_properties = exec_properties,
            **kwargs
        )
