alias(
    name = "cli",
    actual = "//cli/cmd/bb",
)

genquery(
    name = "cli_enterprise_deps",
    expression = "filter(//enterprise, deps(//cli))",
    scope = ["//cli"],
)

sh_test(
    name = "cli_no_enterprise_deps_test",
    srcs = ["cli_no_enterprise_deps_test.sh"],
    args = ["$(rootpath :cli_enterprise_deps)"],
    data = ["cli_enterprise_deps"],
)

exports_files(
    srcs = ["install.sh"],
)

# gazelle:default_visibility //cli:__subpackages__
package(default_visibility = ["//cli:__subpackages__"])
