FROM gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4

# We don't need chromium-browser itself since rules_webtesting provisions the
# chrome binary used for testing, but the rules are not fully hermetic, so we
# install this package only for the shared libraries that it includes as deps.
# See
# https://github.com/bazelbuild/rules_webtesting/issues/416#issuecomment-956350091
RUN apt-get update && apt-get install -y chromium-browser
