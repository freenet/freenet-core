# The Manifest Format

The `freenet.toml` file for each UI component/contract is called its _manifest_.
It is written in the [TOML](https://toml.io/) format. Manifest files consist of
the following sections:

- [[contract]](./manifest.md#the-contract-section) — Defines a contract.
  - [type](./manifest.md#the-type-field) — Contract type.
  - [lang](./manifest.md#the-lang-field) — Contract source language.
  - [output_dir](./manifest.md#the-output_dir-field) — Output path for build
    artifacts.
- [[webapp]](./manifest.md#the-contract-section) — Configuration for UI
  component containers.
- [[state]](./manifest.md#the-state-section) — Optionally seed a state.

## The `[contract]` section

### The `type` field

```toml
[contract]
...
type = "webapp"
```

The type of the contract being packaged. Currently the following types are
supported:

- `standard`, the default type, it can be ellided. This is just a standard
  [contract](./glossary.md#contract).
- `webapp`, a web app [container contract](./glossary.md#container-contract).
  Additionally to the container contract the UI component source will be
  compiled and packaged as the state of the contract.

### The `lang` field

```toml
[contract]
...
lang = "rust"
```

The programming language in which the contract is written. If specified the
build tool will compile the contract. Currently only Rust is supported.

### The `output_dir` field

```toml
[contract]
...
output_dir = "./other/output/dir/"
```

An optional path to the output directory for the build artifacts. If not set the
output will be written to the relative directory `./build/freenet` from the
manifest file directory.

## The `[webapp]` section

An optional section, only specified in case of `webapp` contracts.

### The `lang` field

```toml
[webapp]
...
lang =  "typescript"
```

The programming language in which the web application is written. Currently the
following languages are supported:

- `typescript`, requires [npm](https://www.npmjs.com/) installed.
- `javascript`, requires [npm](https://www.npmjs.com/) installed.

### The `metadata` field

```toml
[webapp]
...
metadata =  "/path/to/metadata/file"
```

An optional path to the metadata for the webapp, if not set the metadata will be
empty.

### The `[webapp.typescript]` options section

Optional section specified in case of the the `typescript` lang.

The following fields are supported:

```toml
[webapp.typescript]
webpack =  true
```

- `webpack` — if set webpack will be used when packaging the contract state.

### The `[webapp.javascript]` options section

Optional section specified in case of the the `javascript` lang.

The following fields are supported:

```toml
[webapp.javascript]
webpack =  true
```

- `webpack` — if set webpack will be used when packaging the contract state.

### The `[webapp.state-sources]` options section

```toml
[webapp.state-sources]
source_dirs =  ["path/to/sources"]
files = ["*/src/**.js"]
```

Specifies the sources for the state of the contract, this will be later on
unpacked and accessible at the HTTP gateway from the Locutus node. Includes any
web sources (like .html or .js files). The `source_dirs` field is a comma
separated array of directories that should be appended to the root of the state,
the `files` field is a comma separated array of
[glob](<https://en.wikipedia.org/wiki/Glob_(programming)>) compatible patterns
to files that will be appendeded to the state.

At least one of `source_dirs`or `files` fields are required.

### The `[webapp.dependencies]` section

```toml
[webapp.dependencies]
...
posts = { path = "../contracts/posts" }
```

An optional list of contract dependencies that will be embedded and available in
the state of the contract. Each entry under this entry represents an alias to
the contract code, it must include a `path` field that specifies the relative
location of the dependency from this manifesto directory.

If dependencies are specified they will be compiled and appended to the contract
state, under the `contracts` directory, and as such, become available from the
HTTP gateway. A `dependencies.json` file will be automatically generated and
placed under such directory that maps the aliases to the file and hash of the
code generated for the dependencies.

In this way the "parent" container contract can use those contracts code to
put/update new values through the websocket API in an ergonomic manner.

## The `[state]` section

```toml
[state]
files = ["*/src/**.js"]
```

An optional section for standard contracts in case they want to seed an state
initially, it will take a single file and make it available at the build
directory.
