# Applies the trust store change from dockerfiles/rbe-ubuntu22-04/Dockerfile to
# an existing Ubuntu-based image that can't easily be rebuilt from source, e.g.
# gcr.io/flame-public/buildbuddy-ci-runner (built by Bazel on Ubuntu 18.04).
# Nothing else in the image changes.
#
# Usage: build with --build-arg BASE=<image>@sha256:<digest>
ARG BASE
FROM ${BASE}

# Distrust all eMudhra (emSign) roots and Sectigo's AAA Certificate Services,
# matching Bazel 9.3.1's bundled JDK, after eMudhra intermediate private keys
# were found to be recoverable (https://emudhra.we-have-a-problem.com/).
# Deselected entries in /etc/ca-certificates.conf survive ca-certificates
# upgrades. Verify with enterprise/tools/build_images/check_distrusted_certs.sh
RUN sed -i -E 's#^(mozilla/(emSign_.*|Comodo_AAA_Services_root)\.crt)$#!\1#' /etc/ca-certificates.conf && \
    ! grep -qE '^mozilla/(emSign_.*|Comodo_AAA_Services_root)\.crt$' /etc/ca-certificates.conf && \
    update-ca-certificates
