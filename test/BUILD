platform(
    name = "linux_arm64",
    constraints = ["//constraints:linux", "//constraints:arm64"],
    exec_properties = {
        "OSFamily": "Linux",
        "Arch": "arm64",
    },
)

sh_test(
    name = "test",
    srcs = ["test.sh"],
    exec_compatible_with = [":linux_arm64"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "OSFamily": "Linux",
        "Arch": "arm64",
    },
)
