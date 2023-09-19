# fdev

A crate for local development purposes.

## Local node example

In order to explore a contract in local mode you need to compile and run the `local-node` executable. The executable
requires a number of input parameters, you can run `local-node local-node-cli --help` in order to see the different options.
Here is an example running the CLI:

```bash
./fdev run-local --input-file /tmp/input --terminal-output --deser-format json "/home/.../freenet/crates/http-gw/examples/test_web_contract.wasm"
```

## Contract state builder example

In order to build an initial state for data or web you need to compile and run the `build_state` executable. The executable requires a number of input parameters,
you can run `local-node contract-state-builder --help` in order to see the different options. Here are some examples running the CLI:

```bash
./fdev build [--input-metadata-path] --input-state-path contracts/freenet-microblogging/view/web --output-file contracts/freenet-microblogging-web/freenet_microblogging_view --contract-type view

./fdev build [--input-metadata-path] --input-state-path contracts/freenet-microblogging/model/ --output-file contracts/freenet-microblogging-data/freenet_microblogging_model --contract-type model
```

Follow the instructions under the `help` command when running the tool in console mode to see the different options and commands to interact
with the contract.
