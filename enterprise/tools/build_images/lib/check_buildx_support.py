from packaging import version
import os
import re
import subprocess
import sys

MIN_DOCKER_VERSION = version.parse("19.0.3")
MIN_KERNEL_VERSION = version.parse("4.8")
MIN_BINFMTS_VERSION = version.parse("2.1.7")

def check_buildx_support(stdout=sys.stdout, stderr=sys.stderr):
    print("Checking prerequisites...", file=stdout)
    # Check to make sure we can use buildx to build the requested platforms
    completed_process = subprocess.run(["docker", "--version"], capture_output=True)
    if completed_process.returncode != 0:
        print("'docker' could not be found. Make sure it is installed and in your PATH.", file=stderr)
        return False

    match = re.fullmatch(
            r"Docker version ([0-9]+(?:[.][0-9]+){0,2})[^,]*, build [0-9]+",
            completed_process.stdout.decode(sys.stdout.encoding).strip(),
            )
    if match != None:
        docker_version = version.parse(match.group(1))
        if docker_version < MIN_DOCKER_VERSION:
            print(f"docker version {docker_version} does not support buildx; need at least docker {MIN_DOCKER_VERSION}.", file=stderr)
            return False
    else:
        print(f"Could not parse docker version; cannot ensure docker is newer than {MIN_DOCKER_VERSION}. If the build fails, check your docker version manually.", file=stderr)

    completed_process = subprocess.run(["docker", "buildx"], capture_output=True)
    if completed_process.returncode != 0:
        completed_process = subprocess.run(["docker", "version"], capture_output=True, check=True)
        match = re.search(
                r"^\s*Experimental:\s*(true|false)$",
                completed_process.stdout.decode(sys.stdout.encoding),
                )
        if match != None:
            if match.group(1) != True:
                print("docker buildx not supported by default; try again with env variable DOCKER_CLI_EXPERIMENTAL=enabled", file=stderr)
                return False
            else:
                print("docker buildx not supported even with experimental features enabled; maybe you don't have the buildx plugin installed?", file=stderr)
                return False
        else:
            print("docker buildx not supported. Unable to detect if experimental features are enabled. You may not have the buildx plugin installed, or you may need to set the env variable DOCKER_CLI_EXPERIMENTAL=enabled, or both.", file=stderr)
            return False

    completed_process = subprocess.run(["uname", "-r"], capture_output=True, check=True)
    match = re.fullmatch(
            r"([0-9]+(?:[.][0-9]+){0,2}).*",
            completed_process.stdout.decode(sys.stdout.encoding).strip(),
            )
    if match != None:
        kernel_version = version.parse(match.group(1))
        if kernel_version < MIN_KERNEL_VERSION:
            print(f"kernel version {kernel_version} does not have binfmt_misc fix-binary (F) support; need at least kernel version {kernel_version}.", file=stderr)
            return False
    else:
        print(f"Could not parse kernel version; cannot ensure kernel is newer than {MIN_KERNEL_VERSION}. If the build fails, check your kernel version manually.", file=stderr)

    completed_process = subprocess.run(["findmnt", "-M", "/proc/sys/fs/binfmt_misc"], capture_output=True)
    if completed_process.returncode != 0:
        print("proc/sys/fs/binfmt_misc is not mounted; mount with 'sudo mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc'", file=stderr)
        return False

    completed_process = subprocess.run(["update-binfmts", "--version"], capture_output=True)
    if completed_process.returncode != 0:
        print("'update-binfmts' could not be found. Make sure it is installed and in your PATH (it may be installed in /usr/sbin).", file=stderr)
        return False
    
    match = re.fullmatch(
            r"binfmt-support\s+([0-9]+(?:[.][0-9]+){0,2}).*",
            completed_process.stdout.decode(sys.stdout.encoding).strip(),
            )
    if match != None:
        binfmts_version = version.parse(match.group(1))
        if binfmts_version < MIN_BINFMTS_VERSION:
            print(f"update-binfmts version {binfmts_version} does not have fix-binary (F) support; need at least docker {MIN_BINFMTS_VERSION}.", file=stderr)
            return False
    else:
        print(f"Could not parse update-binfmts version; cannot ensure update-binfmts is newer than {MIN_BINFMTS_VERSION}. If the build fails, check your update-binfmts version manually.", file=stderr)

    canary_arch = "aarch64"
    if not os.path.exists(f"/proc/sys/fs/binfmt_misc/qemu-{canary_arch}"):
        if not os.path.exists(f"/usr/bin/qemu-{canary_arch}-static"):
            print("QEMU is not installed; install qemu-user-static.", file=stderr)
            return False
        print("QEMU is not registered with binfmt_misc; you may try manually running qemu-binfmt-conf.sh, available in qemu's GitHub repo.", file=stderr)
        return False

    with open(f"/proc/sys/fs/binfmt_misc/qemu-{canary_arch}") as f:
        match = re.search(
                r"^flags:\s*(.*)$",
                f.read(),
                re.MULTILINE,
                )
        if match != None:
            if "F" not in match.group(1):
                print("QEMU is not registered in binfmt_misc with fix-binary (F) flag. Flags: {match.group(1)}", file=stderr)
                return False
        else:
            print(f"Could not find flags in {f.name}; cannot confirm presence of fix-binary (F) flag. If the build fails, check for this flag manually.", file=stderr)

    print("Done.", file=stdout)
    return True

def main():
    return 0 if check_buildx_support() else 1

if __name__ == "__main__":
    main()
