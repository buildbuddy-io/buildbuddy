# Box VM image for `bb box`: the latest workflows image plus coding agents
# (claude, codex), tmux, and the bb CLI.
#
# Build and push with:
#   ./enterprise/tools/build_images/build_image.py \
#     --registry=gcr.io \
#     --repository=flame-public/box-ubuntu24-04 \
#     --tag=latest \
#     --dockerfile=dockerfiles/box-ubuntu24-04/Dockerfile \
#     --platform=amd64

# platform.Ubuntu24_04WorkflowsImage
FROM gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a

ENV DEBIAN_FRONTEND=noninteractive

# tmux, for agent sessions that survive SSH disconnects.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      tmux \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Node.js, required by the agent CLIs.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends \
      nodejs \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Coding agents.
RUN npm install -g @anthropic-ai/claude-code @openai/codex

