FROM gcr.io/flame-public/marketplace/google/ubuntu2004@sha256:41fb71b783c7aa640c991f27c09e81df4d85e7d0e4f746d4f480120a10081b06

ENV DEBIAN_FRONTEND=noninteractive

# Python 2 and 3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python2 \
      python2-dev \
      python3 \
      python3-dev \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# JRE / JDK (headless).
# Don't install recommended packages since that would include
# some alsa/a11y packages which aren't needed for server installs.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      default-jre-headless \
      default-jdk-headless \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# GCC, make
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Misc. utils
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      software-properties-common \
      && \
    add-apt-repository ppa:git-core/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      ed \
      curl \
      file \
      git \
      less \
      openssh-client \
      unzip \
      netcat-traditional \
      wget \
      zip \
      zipmerge \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Clang, LLVM, libcxx
#
# Note: we are using releases maintained by Google since they are much slimmer
# than the apt debs, and because we want to extract to /usr/local to match the
# Ubuntu 16.04 RBE toolchain.
RUN BUCKET="https://storage.googleapis.com/clang-builds-stable" && \
    REVISION="f2b94bd7eaa83d853dc7568fac87b1f8bf4ddec6" && \
    curl -fsSL "$BUCKET/clang-ubuntu20_04/clang_r$REVISION.tar.gz" | \
      tar --directory /usr/local -xzf - && \
    curl -fsSL "$BUCKET/clang-ubuntu20_04/libcxx-msan_r$REVISION.tar.gz" | \
      tar --directory /usr/local -xzf -

# golang
RUN curl -fsSL https://go.dev/dl/go1.19.4.linux-amd64.tar.gz | tar --directory /usr/local -xzf -

# Docker
#
# Note: gnupg is only needed to install Docker, so we uninstall it at the end of
# this step and also run `apt-get autoremove` to get rid of the unnecessary
# packages it came with.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      lsb-release \
      && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo >/etc/apt/sources.list.d/docker.list "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-get update && \
    apt-get install -y \
      docker-ce \
      docker-ce-cli \
      containerd.io \
      && \
    apt-get remove -y gnupg && \
    apt-get autoremove -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# en_US.UTF-8 locale
#
# Bazel forces the locale to be en_US.UTF-8 (not C.UTF-8) for Java actions.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      locales \
      && \
    locale-gen en_US.UTF-8 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Stop trusting the 'emSign ECC Root CA - G3' and 'Comodo AAA Services' roots
# (eMudhra intermediate CA compromise, see
# buildbuddy-io/buildbuddy-internal#8615). Ubuntu 20.04 has no ca-certificates
# package without them, so deselect them ("!" prefix) and regenerate the
# bundle and the Java keystore. This must come after every step that installs
# ca-certificates or Java.
RUN sed -i \
      -e 's|^mozilla/emSign_ECC_Root_CA_-_G3\.crt$|!&|' \
      -e 's|^mozilla/Comodo_AAA_Services_root\.crt$|!&|' \
      /etc/ca-certificates.conf && \
    update-ca-certificates

# Fail the build if the 'emSign ECC Root CA - G3' or 'Comodo AAA Services'
# root is still trusted (eMudhra intermediate CA compromise, see
# buildbuddy-io/buildbuddy-internal#8615): selected in /etc/ca-certificates.conf,
# linked in /etc/ssl/certs, or present by SHA-256 fingerprint in
# ca-certificates.crt or any Java cacerts keystore in the image.
RUN <<'EOF'
set -eu
fail() { echo "CA root check: $*" >&2; exit 1; }
tmp=$(mktemp -d)
awk -v d="$tmp" '/BEGIN CERTIFICATE/ { if (f) close(f); f = d "/" (++n) ".pem" } f { print > f }' /etc/ssl/certs/ca-certificates.crt
for f in "$tmp"/*.pem; do openssl x509 -in "$f" -noout -fingerprint -sha256; done | cut -d= -f2 >"$tmp/bundle"
[ -s "$tmp/bundle" ] || fail "no certificates in /etc/ssl/certs/ca-certificates.crt"
: >"$tmp/keystores"
for ks in $(find / -xdev -name cacerts \( -type f -o -type l \) -exec readlink -f {} \; | sort -u); do
  command -v keytool >/dev/null || fail "$ks exists but keytool is not installed"
  keytool -list -v -keystore "$ks" -storepass changeit >"$tmp/ks" 2>&1 || fail "keytool cannot read $ks"
  grep -q '^Alias name:' "$tmp/ks" || fail "no entries in $ks"
  cat "$tmp/ks" >>"$tmp/keystores"
  echo "checked $ks"
done
for root in \
    emSign_ECC_Root_CA_-_G3=86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B \
    Comodo_AAA_Services_root=D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4; do
  name=${root%%=*}
  fp=${root#*=}
  ! grep -qxF "mozilla/$name.crt" /etc/ca-certificates.conf || fail "$name is selected in /etc/ca-certificates.conf"
  ! [ -e "/etc/ssl/certs/$name.pem" ] && ! [ -L "/etc/ssl/certs/$name.pem" ] || fail "/etc/ssl/certs/$name.pem exists"
  ! grep -qxF "$fp" "$tmp/bundle" || fail "$name is in /etc/ssl/certs/ca-certificates.crt"
  ! grep -qF "$fp" "$tmp/keystores" || fail "$name is in a Java cacerts keystore"
done
echo "CA root check passed: $(wc -l <"$tmp/bundle") certificates in bundle"
rm -rf "$tmp"
EOF

# Alias `python` to `python2` to be compatible with the Ubuntu 16.04 RBE image.
# Note, after Ubuntu 20.04, we should probably install `python-is-python3` instead.
RUN update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1

# Set up env

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

ENV ASAN_SYMBOLIZER_PATH=/usr/local/bin/llvm-symbolizer
ENV MSAN_SYMBOLIZER_PATH=/usr/local/bin/llvm-symbolizer
ENV TSAN_SYMBOLIZER_PATH=/usr/local/bin/llvm-symbolizer
ENV UBSAN_SYMBOLIZER_PATH=/usr/local/bin/llvm-symbolizer

ENV CC=/usr/local/bin/clang
ENV GCOV=/dev/null
ENV LD_LIBRARY_PATH=/usr/local/lib

ENV LC_ALL=C.UTF-8
ENV LANGUAGE=C.UTF-8
ENV LANG=C.UTF-8

ENV PATH="$PATH:/usr/local/go/bin"

CMD bash
