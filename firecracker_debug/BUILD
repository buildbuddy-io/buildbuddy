sh_test(
    name = "docker_in_firecracker_test",
    srcs = ["exit0.sh"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
	# "test.init-dockerd": "true",
    },
)
