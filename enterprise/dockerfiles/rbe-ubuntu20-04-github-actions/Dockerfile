# TODO: build a VM image from
# https://github.com/actions/runner-images/tree/main/images/ubuntu

FROM gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:271e5e3704d861159c75b8dd6713dbe5a12272ec8ee73d17f89ed7be8026553f

# Install GitHub Actions runner and required system dependencies
RUN apt-get update && \
    apt-get install -y curl && \
    mkdir /actions-runner && \
    cd /actions-runner && \
    VERSION=2.313.0 && \
    curl -O -L https://github.com/actions/runner/releases/download/v${VERSION}/actions-runner-linux-x64-${VERSION}.tar.gz && \
    tar xzf ./actions-runner-linux-x64-${VERSION}.tar.gz && \
    rm ./actions-runner-linux-x64-${VERSION}.tar.gz && \
    ./bin/installdependencies.sh && \
    chown -R 1000:1000 /actions-runner && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
