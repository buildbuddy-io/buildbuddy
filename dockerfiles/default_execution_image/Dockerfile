FROM gcr.io/cloud-marketplace/google/rbe-ubuntu16-04@sha256:f6568d8168b14aafd1b707019927a63c2d37113a03bcee188218f99bd0327ea1

RUN apt-get update && apt-get install -y rpm build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.4/bazelisk-linux-amd64 && chmod +x /usr/local/bin/bazelisk && /usr/local/bin/bazelisk

# Install docker.
RUN apt-get update && apt-get install -y apt-transport-https software-properties-common && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
    chmod a+r /etc/apt/keyrings/docker.asc && \
		echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y docker-ce && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
