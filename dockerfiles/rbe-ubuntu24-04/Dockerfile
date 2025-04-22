FROM marketplace.gcr.io/google/ubuntu2404@sha256:f226b52ce8f8365aa0a52fb8f4392ecbe1c215639f73edf16a12e5f3d6765147

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

CMD bash
