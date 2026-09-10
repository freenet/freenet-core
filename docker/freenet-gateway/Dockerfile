FROM node:26-bookworm-slim

# Install required dependencies
RUN DEBIAN_FRONTEND=noninteractive \
  && apt-get update \
  && apt-get upgrade -yqq \
  && apt-get install --no-install-recommends -y \
  build-essential \
  curl \
  ca-certificates \
  git \
  openssl \
  libssl-dev \
  pkg-config \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup target add wasm32-unknown-unknown

# Set up environment variables
ENV FREENET_DIR="/root/freenet"
ENV BASE_DIR="/root/.cache/freenet"
ENV KEYS_DIR="${BASE_DIR}/keys"
ENV RUST_BACKTRACE=1
ENV RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

# Clone the Freenet repository
WORKDIR $FREENET_DIR
RUN git clone https://github.com/freenet/freenet-core.git "$FREENET_DIR/freenet-core" \
  && cd "$FREENET_DIR/freenet-core" \
  && git config --global url."https://github.com/".insteadOf "git@github.com:" \
  && git submodule update --init --recursive

# Build Freenet
#
# CARGO_BUILD_JOBS caps rustc/linker parallelism. Left unset, cargo defaults to
# one job per logical core, and this workspace pulls in enough heavyweight
# dependencies (wasmtime among them) that full-width parallelism can exceed
# available memory on smaller build hosts and get the build OOM-killed. Left
# as a build arg (default empty = cargo's own default) rather than a fixed
# number, so CI and larger machines keep full parallelism unless a builder
# opts into a cap.
WORKDIR $FREENET_DIR/freenet-core/crates/core
ARG CARGO_BUILD_JOBS=
RUN if [ -n "$CARGO_BUILD_JOBS" ]; then \
      cargo install --path . --jobs "$CARGO_BUILD_JOBS"; \
    else \
      cargo install --path .; \
    fi

# Copy the gateway startup script
COPY freenet-gateway-startup.sh /usr/local/bin/freenet-gateway-startup.sh
RUN chmod +x /usr/local/bin/freenet-gateway-startup.sh

# Copy the keypair generation script
COPY generate-keypair.sh /usr/local/bin/generate-keypair.sh
RUN chmod +x /usr/local/bin/generate-keypair.sh

# Generate keypair
RUN /usr/local/bin/generate-keypair.sh

# Expose default gateway ports
EXPOSE 31337

# Use the gateway startup script as the entrypoint
CMD ["/usr/local/bin/freenet-gateway-startup.sh"]