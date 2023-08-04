load(":data.bzl", "GO_MODULES")

def firecracker_sh_test(name, script, recycle = False, cpus = 3):
    native.sh_test(
        name = name,
        srcs = [script],
        exec_properties = {
            "container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:eb81189af1cec44b5348751f75ec7aea516d0de675b8461235f4cc4e553a34b5",
            "workload-isolation-type": "firecracker",
            "EstimatedCPU": str(cpus),
            "EstimatedMemory": "8GB",
            "EstimatedFreeDiskBytes": "10000000000",
            "recycle-runner": "true" if recycle else "false",
        },
        env = {
            "GO_MODULES": GO_MODULES,
            "CPU_COUNT": str(cpus),
        },
    )
