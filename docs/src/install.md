# Build freenet from source On Linux:

There is a single line command to build all freenet on Linux.

```bash
wget https://sh.rustup.rs ; sh index.html -y && source "$HOME/.cargo/env" && rustup default stable && rustup target add wasm32-unknown-unknown && (sh curl -L https://git.io/n-install | bash) ; ~/n/bin/n latest ; ~/n/bin/npm install -g typescript webpack && git clone https://github.com/freenet/freenet-core/ &&    cd freenet-core &&  git submodule update --init --recursive &&         export CARGO_TARGET_DIR="$(pwd)/target" && cd stdlib/typescript/ && npm run dev.package && cd ../.. &&        cargo install --path crates/core --force &&     cargo install --path crates/fdev --force         && cd ./modules/identity-management/ && make build &&  cd ../antiflood-tokens/ && rm Cargo.lock ; make build &&      cd ../../apps/freenet-email-app && make build
```
