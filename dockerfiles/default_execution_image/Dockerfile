FROM gcr.io/cloud-marketplace/google/rbe-ubuntu16-04@sha256:5464e3e83dc656fc6e4eae6a01f5c2645f1f7e95854b3802b85e86484132d90e

RUN apt-get update && apt-get install -y rpm build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.4/bazelisk-linux-amd64 && chmod +x /usr/local/bin/bazelisk && /usr/local/bin/bazelisk

# Install docker.
# Note: Some apt utils depend on /usr/bin/python3 being symlinked to python3.5, but the
# google RBE image has python3 symlinked to python3.6. As a workaround, we temporarily
# link /usr/bin/python3 to /usr/bin/python3.5 during this step.
RUN mv /usr/bin/python3 /usr/bin/python3.bak && ln -s /usr/bin/python3.5 /usr/bin/python3 && \
    apt-get update && apt-get install -y apt-transport-https software-properties-common && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable" && \
    apt-get update && apt-get install -y docker-ce && \
    mv /usr/bin/python3.bak /usr/bin/python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
