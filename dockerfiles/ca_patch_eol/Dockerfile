# Minimal CA patch for end-of-life Ubuntu images (16.04, 18.04) that no longer
# get ca-certificates updates. It adds one layer on top of the unchanged base
# that deselects the roots behind the compromised eMudhra intermediates
# (buildbuddy-io/buildbuddy-internal#8615) and regenerates
# /etc/ssl/certs/ca-certificates.crt (and, via the ca-certificates-java hook,
# /etc/ssl/certs/java/cacerts). No packages are installed or upgraded.
#
# CERTS lists the ca-certificates.conf entries to deselect. Every entry must be
# present and selected in the base image, or the build fails. Ubuntu 16.04's
# bundle predates the emSign roots, so only the Comodo AAA root applies there:
#
#   docker build \
#     --build-arg BASE=gcr.io/flame-public/executor-docker-default@sha256:c346c46f3333d7d7f102c6e19a36bcf8403de465ad3cec91c2265a4d56987124 \
#     --build-arg CERTS=mozilla/Comodo_AAA_Services_root.crt \
#     dockerfiles/ca_patch_eol
#
#   docker build \
#     --build-arg BASE=gcr.io/flame-public/buildbuddy-ci-runner@sha256:8cf614fc4695789bea8321446402e7d6f84f6be09b8d39ec93caa508fa3e3cfc \
#     dockerfiles/ca_patch_eol
#
# Neither base image sets USER, so this file doesn't either. If you use it on a
# base that does, add the base's USER back after the RUN step.
ARG BASE
FROM ${BASE}

ARG CERTS="mozilla/emSign_ECC_Root_CA_-_G3.crt mozilla/Comodo_AAA_Services_root.crt"

RUN set -eu; \
    test -n "$(echo ${CERTS})" || { echo "CERTS build arg is empty" >&2; exit 1; }; \
    for c in ${CERTS}; do \
      test -f "/usr/share/ca-certificates/$c" || { echo "missing /usr/share/ca-certificates/$c" >&2; exit 1; }; \
      grep -qxF "$c" /etc/ca-certificates.conf || { echo "$c is not selected in /etc/ca-certificates.conf" >&2; exit 1; }; \
      sed -i "s|^$c\$|!$c|" /etc/ca-certificates.conf; \
      grep -qxF "!$c" /etc/ca-certificates.conf; \
    done; \
    update-ca-certificates; \
    bundle="$(tr -d '\n' < /etc/ssl/certs/ca-certificates.crt)"; \
    for c in ${CERTS}; do \
      pem="/etc/ssl/certs/$(basename "$c" .crt).pem"; \
      test ! -e "$pem" || { echo "$pem still present" >&2; exit 1; }; \
      body="$(grep -v -- '-----' "/usr/share/ca-certificates/$c" | tr -d '\n')"; \
      case "$bundle" in *"$body"*) echo "$c still in ca-certificates.crt" >&2; exit 1;; esac; \
    done; \
    dangling="$(find -L /etc/ssl/certs -maxdepth 1 -type l)"; \
    test -z "$dangling" || { echo "dangling symlinks in /etc/ssl/certs: $dangling" >&2; exit 1; }; \
    jks=/etc/ssl/certs/java/cacerts; \
    if [ -e "$jks" ]; then \
      if command -v keytool >/dev/null 2>&1; then \
        keytool -list -keystore "$jks" -storepass changeit >/dev/null; \
        for c in ${CERTS}; do \
          alias="debian:$(basename "$c" .crt | tr 'A-Z' 'a-z').pem"; \
          if keytool -list -keystore "$jks" -storepass changeit -alias "$alias" >/dev/null 2>&1; then \
            echo "$alias still in $jks" >&2; exit 1; \
          fi; \
          echo "verified $alias absent from $jks"; \
        done; \
      else \
        echo "WARNING: $jks exists but keytool is not available; not checked" >&2; \
      fi; \
    fi
