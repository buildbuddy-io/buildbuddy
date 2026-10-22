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
#
# The apt key is pinned by fingerprint so that a tampered download fails the
# build instead of letting arbitrary packages in.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      lsb-release \
      && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /tmp/docker.asc && \
    fprs=$(gpg --show-keys --with-colons /tmp/docker.asc | awk -F: '$1=="pub"{p=1} $1=="fpr"&&p{print $10; p=0}') && \
    { [ "$fprs" = "9DC858229FC7DD38854AE2D88D81803C0EBFCD88" ] || { echo "unexpected Docker apt key(s): $fprs" >&2; exit 1; }; } && \
    gpg --dearmor -o /etc/apt/keyrings/docker.gpg /tmp/docker.asc && \
    rm /tmp/docker.asc && \
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

# Fail if the eMudhra-compromised roots are still trusted, e.g. from a stale cached apt layer (buildbuddy-io/buildbuddy-internal#8615).
RUN subjects=$(openssl crl2pkcs7 -nocrl -certfile /etc/ssl/certs/ca-certificates.crt | openssl pkcs7 -print_certs -noout | grep '^subject=') && \
    [ "$(echo "$subjects" | wc -l)" -gt 100 ] && \
    if echo "$subjects" | grep -E 'emSign ECC Root CA - G3|AAA Certificate Services'; then exit 1; fi

CMD bash
