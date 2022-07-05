# locutus-dev
A crate for local development purposes.

## Local node example

In order to explore a conract in local mode you need to compile and run the `local-node` executable. The executable requires a number of input parameters, you can run `local-node --help` in order to see the different options. Here is an example running the CLI:
```
$ ./local-node --input-file /tmp/input --terminal-output --deser-format json "/home/.../locutus/crates/http-gw/examples/test_web_contract.wasm"
```
Follow the instructions under the `help` command when running the tool in console mode to see the different options and commands to interact with the contract.

## Contract state builder example

In order to build an initial state for data or web you need to compile and run the `build_state` executable. The executable requires a number of input parameters, you can run `build_state --help` in order to see the different options. Here are some examples running the CLI:
```
$ ./build_state --input-path contracts/freenet-microblogging-web/web --output-file contracts/freenet-microblogging-web/encoded_web_state --contract-type web

$ ./build_state --input-path contracts/freenet-microblogging-data/ --output-file contracts/tfreenet-microblogging-data/encoded_data_state --contract-type data
```

Follow the instructions under the `help` command when running the tool in console mode to see the different options and commands to interact with the contract.
