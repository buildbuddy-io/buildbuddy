FROM marketplace.gcr.io/google/ubuntu2204@sha256:216c2da9e2b38855972398dc75335f1a53ec48c6bce7f2699d9a9d0697d6cb43

ENV DEBIAN_FRONTEND=noninteractive

# Dependencies needed to build Firecracker guest kernels. Install the matching
# gcc/binutils pair for the current image arch.
RUN case "$(uname -m)" in \
      x86_64) toolchain_packages="binutils-x86-64-linux-gnu gcc-x86-64-linux-gnu" ;; \
      aarch64) toolchain_packages="binutils-aarch64-linux-gnu gcc-aarch64-linux-gnu" ;; \
      *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
    esac && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      ${toolchain_packages} \
      bc \
      bison \
      build-essential \
      ca-certificates \
      cpio \
      dwarves \
      file \
      flex \
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
