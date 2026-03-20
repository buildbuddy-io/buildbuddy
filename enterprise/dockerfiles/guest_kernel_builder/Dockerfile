FROM marketplace.gcr.io/google/ubuntu2204@sha256:216c2da9e2b38855972398dc75335f1a53ec48c6bce7f2699d9a9d0697d6cb43

ENV DEBIAN_FRONTEND=noninteractive

# Dependencies needed to build Firecracker guest kernels for x86_64 and arm64.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      bc \
      binutils-aarch64-linux-gnu \
      binutils-x86-64-linux-gnu \
      bison \
      build-essential \
      ca-certificates \
      cpio \
      dwarves \
      file \
      flex \
      gcc-aarch64-linux-gnu \
      gcc-x86-64-linux-gnu \
      git \
      kmod \
      libelf-dev \
      libssl-dev \
      make \
      python3 \
      rsync \
      wget \
      xz-utils \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

CMD bash
