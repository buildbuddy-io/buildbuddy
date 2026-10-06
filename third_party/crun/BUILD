package(default_visibility = ["//visibility:public"])

genrule(
    name = "crun",
    srcs = ["@com_github_containers_crun//:srcs"],
    outs = ["bin/crun"],
    cmd = """
        set -euo pipefail
        out="$$PWD/$@"
        for file in $(SRCS); do
            if [[ "$$file" == */configure ]]; then
                src="$${file%/configure}"
                break
            fi
        done
        build_dir="$$(mktemp -d)"
        trap 'rm -rf "$$build_dir"' EXIT
        cp -RL "$$src/." "$$build_dir/"
        chmod -R u+w "$$build_dir"
        cd "$$build_dir"
        cp .tarball-git-version.h git-version.h
        ./configure --disable-systemd --disable-shared --disable-dl \\
            --disable-criu --disable-maintainer-mode --enable-embedded-blake3 \\
            CFLAGS=-O2 LDFLAGS=-static
        make -j2 crun CRUN_LDFLAGS=-all-static
        # Reject an ELF interpreter or shared library dependency.
        readelf -lWd crun > elf.txt
        if grep -Eq 'INTERP|NEEDED' elf.txt; then
            cat elf.txt >&2
            exit 1
        fi
        cp crun "$$out"
    """,
    exec_compatible_with = [
        "@platforms//os:linux",
    ],
    exec_properties = {
        "container-image": "docker://gcr.io/flame-public/crun-builder@sha256:d7991b17cab70ae76dda20a6db619354705ca25caf49fdfb0b90941628eb94cb",
        "EstimatedComputeUnits": "2",
    },
    executable = True,
    target_compatible_with = ["@platforms//os:linux"],
)
