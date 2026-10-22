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
#
# Nobody publishes checksums for these objects, so the sha256s were taken from
# a download whose md5 matched the GCS object metadata (generations
# 1658922556226005 and 1658922556176128, uploaded 2022-07-27).
RUN BUCKET="https://storage.googleapis.com/clang-builds-stable" && \
    REVISION="f2b94bd7eaa83d853dc7568fac87b1f8bf4ddec6" && \
    curl -fsSL "$BUCKET/clang-ubuntu20_04/clang_r$REVISION.tar.gz" -o /tmp/clang.tar.gz && \
    echo "8b04faa905b43eb274c216f21d8367700a787351db15ba04967113609ec4cdbc  /tmp/clang.tar.gz" | sha256sum -c - && \
    tar --directory /usr/local -xzf /tmp/clang.tar.gz && \
    curl -fsSL "$BUCKET/clang-ubuntu20_04/libcxx-msan_r$REVISION.tar.gz" -o /tmp/libcxx-msan.tar.gz && \
    echo "d48e9777f63b6e76d4fb81cb440c3e56a078ffa5e2788ca823fc2bf43a9a5edf  /tmp/libcxx-msan.tar.gz" | sha256sum -c - && \
    tar --directory /usr/local -xzf /tmp/libcxx-msan.tar.gz && \
    rm /tmp/clang.tar.gz /tmp/libcxx-msan.tar.gz

# golang (sha256 from https://go.dev/dl/?mode=json&include=all)
RUN curl -fsSL https://go.dev/dl/go1.19.4.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
    echo "c9c08f783325c4cf840a94333159cc937f05f75d36a8b307951d5bd959cf2ab8  /tmp/go.tar.gz" | sha256sum -c - && \
    tar --directory /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz

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

# Fail if the eMudhra-compromised roots are still trusted (buildbuddy-io/buildbuddy-internal#8615).
RUN for args in "-printcert -file /etc/ssl/certs/ca-certificates.crt" "-list -cacerts -storepass changeit"; do \
      fps=$(keytool $args | grep -oE '([0-9A-F]{2}:){31}[0-9A-F]{2}'); \
      [ "$(echo "$fps" | wc -l)" -gt 100 ] || { echo "too few certificates from keytool $args" >&2; exit 1; }; \
      if echo "$fps" | grep -Fx \
          -e 86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B \
          -e D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4; then \
        echo "compromised root still trusted (keytool $args)" >&2; exit 1; \
      fi; \
    done

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
