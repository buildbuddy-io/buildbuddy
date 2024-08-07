FROM busybox@sha256:9ae97d36d26566ff84e8893c64a6dc4fe8ca6d1144bf5b87b2b85a32def253c7

# Test layer ordering: later edit to the file should be reflected
RUN mkdir -p /test/ && echo 1 > /test/foo
RUN mkdir -p /test/ && echo 2 > /test/foo

# Test directory whiteouts
RUN mkdir -p /test/DELETED_DIR/subdir
RUN rm -rf /test/DELETED_DIR

# Test file whiteouts
RUN mkdir -p /test/ && touch /test/DELETED_FILE
RUN rm /test/DELETED_FILE

# Test setting env vars
ENV TEST_ENV_VAR=foo

# Test appending to existing env vars
ENV PATH="$PATH:/test/bin"
