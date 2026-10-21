FROM gcr.io/flame-public/marketplace/google/ubuntu2404@sha256:0da15ca48f0672879e2146fd8c5ca11f65cfd023c9cfd2ebb80e673fc4bcd96c

ENV DEBIAN_FRONTEND=noninteractive

# Python 3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-dev \
      python-is-python3 \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# gpg-agent (for add-apt-repository below)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gpg-agent \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/* \
    && \
    mkdir -p /root/.gnupg

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
    add-apt-repository ppa:git-core/ppa -y && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      cmake \
      ed \
      file \
      git \
      less \
      netcat-traditional \
      openssh-client \
      sudo \
      unzip \
      wget \
      zip \
      zipmerge \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

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
      docker-ce=5:28.1.0-1~ubuntu.24.04~noble \
      docker-ce-cli=5:28.1.0-1~ubuntu.24.04~noble \
      containerd.io=1.7.27-1 \
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

# The new iptables requires nft which we haven't yet set up properly in our VM
# guest kernel configs. This prevents docker network setup from working properly
# when using docker-in-fireecracker. To work around this, downgrade to
# iptables-legacy for now.
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy && \
    update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# Provision a non-root user named "buildbuddy" and set up passwordless sudo.
# Non-root users are needed for some bazel toolchains, such as hermetic python.
# Also add them to the docker group so they can use docker.
RUN useradd --create-home buildbuddy --groups sudo,docker && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

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

CMD bash
