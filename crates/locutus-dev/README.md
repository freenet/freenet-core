# locutus-dev
A crate for local development purposes.

## Example

In order to explore a conract in local mode you need to compile and run the `local-node` executable. The executable requires a number of input parameters, you can run `local-node --help` in order to see the different options. Here is an example running the CLI:
```
$ ./local-node --input-file /tmp/input --terminal-output --deser-format json "/home/.../locutus/crates/http-gw/examples/test_web_contract.wasm"
```

Follow the instructions under the `help` command when running the tool in console mode to see the different options and commands to interact with the contract.
