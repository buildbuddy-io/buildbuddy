sh_test(
    name = "hog_cpu_test",
    srcs = ["hog_cpu.sh"],
    exec_properties = {
        # "test.container-image": "docker://gcr.io/flame-public/stress-ng",
        "test.EstimatedComputeUnits": "1",
        "test.EstimatedMemory": "100MB",
    },
    tags = ["manual"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
