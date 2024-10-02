FROM marketplace.gcr.io/google/ubuntu2204@sha256:b70be2d80535d3fe99e2bc484133da588728add8fc15956d124c478b42e61f2c

ENV DEBIAN_FRONTEND=noninteractive

# Python 2 and 3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python2 \
      python2-dev \
      python3 \
      python3-dev \
      python-is-python3 \
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

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gpg-agent \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/* \
    && \
    mkdir -p mkdir -p /root/.gnupg

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
