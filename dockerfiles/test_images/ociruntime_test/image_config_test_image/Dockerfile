FROM mirror.gcr.io/alpine

# Test layer ordering: later edit to the file should be reflected
RUN mkdir -p /test/ && echo 1 > /test/foo
RUN mkdir -p /test/ && echo 2 > /test/foo

# Test directory whiteouts
RUN mkdir -p /test/DELETED_DIR/subdir
RUN rm -rf /test/DELETED_DIR

# Test file whiteouts
RUN mkdir -p /test/ && touch /test/DELETED_FILE
RUN rm /test/DELETED_FILE

# Add the following user setup:
# - 'buildbuddy' user, ID 1000
# - 'basil' user, ID 1001, with extra group membership in 'auxgroup' group
RUN apk update && apk add --no-cache shadow && rm -rf /var/cache/apk/* && \
    useradd --create-home buildbuddy && \
    useradd --create-home basil && \
    groupadd auxgroup && \
    usermod -aG auxgroup basil
USER buildbuddy

# Test setting env vars
ENV TEST_ENV_VAR=foo

# Test appending to existing env vars
ENV PATH="$PATH:/test/bin"
