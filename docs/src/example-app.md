# Build freenet from source on Linux:

There is a single line command to build all freenet on Linux.

```
git clone https://github.com/freenet/freenet-core && .\build-all.sh
```

then you could test it by building and deploying any web application:

```
.\build-examples.sh
```
.


# Build freenet from source on Linux from command line:

You could also run this:

```bash
wget https://sh.rustup.rs ; sh index.html -y &&\
  source "$HOME/.cargo/env" &&\
  rustup default stable &&\
  rustup target add wasm32-unknown-unknown &&\
  (sh curl -L https://git.io/n-install | bash) ; ~/n/bin/n latest ; ~/n/bin/npm install -g typescript webpack &&\
  git clone https://github.com/freenet/freenet-core/ &&\
  cd freenet-core &&\
  git submodule update --init --recursive &&\
  export CARGO_TARGET_DIR="$(pwd)/target" &&\
  cd stdlib/typescript/ &&\
  npm run dev.package &&\
  cd ../.. &&\
  cargo install --path crates/core --force &&\
  cargo install --path crates/fdev --force &&\
  cd ./modules/identity-management/ &&\
  make build &&\
  cd ../antiflood-tokens/ &&\
  rm Cargo.lock ; make build &&\
  cd ../../apps/freenet-email-app &&\
  make build
```

Let's decompose this:

Install rust:

```bash
wget https://sh.rustup.rs ; sh index.html -y && source "$HOME/.cargo/env"
```

then select rustup toolchain and add webasm target with:

```
rustup default stable && rustup target add wasm32-unknown-unknown
```

Install `n` tu update typescript latter:

```
sh curl -L https://git.io/n-install | bash)
```

Update `npm`. Warning: required! You must do that to have the latest npm packages.

```
~/n/bin/n latest
```

Install typescript and `webpack` to `build` `freenet-email-app` example.
```
~/n/bin/npm install -g typescript webpack
```

clone the project:

`git clone https://github.com/freenet/freenet-core/ && cd freenet-core &&  git submodule update --init --recursive`

set the build environment part (mandatory):

```
export CARGO_TARGET_DIR="$(pwd)/target"
```

build typescript stdlib.
```
cd stdlib/typescript/ && npm run dev.package && cd ../..
```

Finally compile freenet:

```
cargo install --path crates/core --force
cargo install --path crates/fdev --force
```

# Build and deploy freenet contract examples:

Build identity managemenet and antiflood token freent modules:

```
cd ./modules/identity-management/
make build
cd ../antiflood-tokens/
```


Fix a compile issue with:
```
rm Cargo.lock
```


```
make build
```

```
cd ../../apps/freenet-email-app
make build
```
