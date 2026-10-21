# For bases whose apt archive has the fixed ca-certificates package (Ubuntu
# 22.04, 24.04). Adds one layer that installs exactly CA_VERSION, so the build
# fails if that package isn't available, then checks the roots are gone.
# Build from this directory; see README.md.
# None of the bases set USER. If one does, add it back after the RUN step.
ARG BASE
FROM ${BASE}

ARG CA_VERSION
ARG CERTS="mozilla/emSign_ECC_Root_CA_-_G3.crt mozilla/Comodo_AAA_Services_root.crt"

RUN --mount=type=bind,source=check_roots_removed.sh,target=/ca_patch_check.sh \
    set -eu; \
    test -n "${CA_VERSION}" || { echo "CA_VERSION build arg is required" >&2; exit 1; }; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "ca-certificates=${CA_VERSION}"; \
    rm -rf /var/lib/apt/lists/*; \
    sh /ca_patch_check.sh ${CERTS}
