# Ubuntu 26.04, which also has the e2fsprogs version that reproducible ext4
# images need.
FROM mirror.gcr.io/library/ubuntu@sha256:da6fc2be547864451aa253836dd926da33623312df4a9a243e35dc877c378a78

RUN apt-get update && \
    apt-get install -y --no-install-recommends erofs-utils && \
    rm -rf /var/lib/apt/lists/*
