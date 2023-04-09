# Create a Freenet Application

This guide will walk through how to develop a simple distributed web application
using Locutus. To do that, we'll be using Rust for the contracts themselves and
Typescript for developing the web application.

At the time of writing (September 2022) the Locutus network is not yet active.
We've published this guide so that people can experiment with building and
running Locutus applications locally, and provide
[feedback](https://github.com/freenet/locutus/issues).

You can see some examples of working applications and contracts in the `apps`
directory of the locutus repository, e.g.:

- [freenet-microblogging](https://github.com/freenet/locutus/tree/main/apps/freenet-microblogging)
  (WIP)

## Installation

Development for Locutus requires installing some dependencies:

### 1. Rust & Cargo

Locutus is developed in [Rust](https://www.rust-lang.org/), on Linux/Mac this
will install Rust and its build tool
[Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html) which
Locutus also requires:



### 2. Locutus Dev Tool (LTD)

Once you have a working installation of Cargo you can install the Locutus dev
tools:

```bash
$ cargo install locutus
```

This command will install `ldt` (Locutus Dev Tool) and a working node that can
be used for local development.

#### 2.1 Usage

You can find more information about the available commands by executing `ldt`
with the `--help` argument:

```
$ ldt --help

Locutus Development Tool 0.0.2
The Freenet Project Inc.

USAGE:
    ldt [DATA_DIR] <SUBCOMMAND>

ARGS:
    <DATA_DIR>    Overrides the default data directory where Locutus files are stored

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    build        Builds and packages a contract
    execute      Node CLI
    help         Print this message or the help of the given subcommand(s)
    new          Create a new Locutus contract and/or app
    publish      Publishes a new contract to the network
    run-local    A CLI utility for testing out contracts against a Locutus local node
```

## Creating a new contract

You can create a new [contract](glossary.md#contract) skeleton by executing the
`new` command with `ldt`. Two contract types are supported currently by the
tool, regular [contracts](glossary.md#contract), and [web
application](glossary.md#web-application) [container
contracts](glossary.md#container-contract). Currently, the following
technological stacks are supported (more to be added in the future):

- Regular contracts:
  - Rust (_default_)
- Web applications:
  - Container development:
    - Rust (_default_)
  - Web/state development:
    - Typescript. (_default: using npm and webpack_)
    - JavaScript.
    - Rust (**WIP**).

We will need to create a directory that will hold our web app and initialize it:

```bash
$ mkdir -p my-app/web
$ mkdir -p my-app/backend
$ cd my-app/web
$ ldt new web-app
```

will create the skeleton for a web application and its container contract for
Locutus ready for development at the `my-app/web` directory.

## Making a container contract

The first thing that we need is to write the code for our container contract.
This contract's role is to contain the web application code itself, allowing it
to be distributed over Locutus.

The `new` command has created the source ready to be modified for us, in your
favorite editor open the following file:

```bash
$ ./container/src/lib.rs
```

In this case, and for simplicity's sake, the contract won't be performing any
functions, but in a realistic scenario, this contract would include some basic
security functionality like verifying that whoever is trying to update the
contract has the required credentials.

To make our contract unique so it doesn't collide with an existing contract, we
can generate a random signature that will be embedded with the contract.

<!--
What would happen in case of a collision with an existing contract? (That would be if we try to publish a contract that has the same combination of code and parameters.) Then it would fail to publish our contract in the network and would get a rejection because we would be trying to update an existing contract. And we would have to make a slight change in the code/parameters so this collision is avoided. To make this work, there needs to exist a type, which requires (this can be only done once, at the top level of the library crate) implementing the `ContractInterface` trait from `locutus-stdlib`.
-->

For example in the `lib.rs` file we will write the following:

```rust,no_run,noplayground
{{#include ../../crates/locutus-runtime/examples/contract.rs:contractifce}}
```

That's a lot of information, let's unpack it:

```rust,noplayground
use locutus_stdlib::prelude::*;
```

Here we are importing the necessary types and traits to write a Locutus contract
successfully using Rust.

```rust,noplayground
pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];
```

This will make our contract unique, notice the `pub` qualifier so the compiler
doesn't remove this constant because is unused and is included in the output of
the compiler.

```rust,noplayground
struct Contract;

#[contract]
impl ContractInterface for Contract {
  ...
}
```

<!--
TODO: Elsewhere in the documentation, explain the intricate details of how interfacing through WASM works. In theory users could implement their own wrapping code as long as the follow the low level WASM code specification.
-->

Here we create a new type, `Contract` for which we will be implementing the
`ContractInterface` trait. To know more details about the functionality of a
contract, delve into the details of the [contract
interface](contract-interface.md).

Notice the `#[contract]` macro call, this will generate the necessary code for
the WASM runtime to interact with your contract ergonomically and safely. Trying
to use this macro more than once in the same module will result in a compiler
error, and only the code generated at the top-level module will be used by the
runtime.

As a rule of thumb, one contract will require implementing the
`ContractInterface`` exactly once.

### Creating a web application

Now we have a working example of a contract, but our contract is an empty shell,
which does not do anything yet. To change this, we will start developing our web
application.

To do that, we can go and modify the code of the contract state, which in this
case is the web application. Locutus offers a standard library (stdlib) that can
be used with Typescript/JavaScript to facilitate the development of web
applications and interfacing with your local node, so we will make our
`package.json` contains the dependency:

```
{
  "dependencies": {
    "@locutus/locutus-stdlib": "0.0.2"
  }
}
```

Open the file `src/index.ts` in a code editor and you can start developing the
web application.

An important thing to notice is that our application will need to interface with
our local node, the entry point for our machine to communicate with other nodes
in the network. The stdlib offers a series of facilities in which you will be
able to communicate with the network ergonomically.

Here is an example of how you could write your application to interact with the
node:

```typescript
import { LocutusWsApi } from "@locutus/locutus-stdlib/webSocketInterface";

const handler = {
  onPut: (_response: PutResponse) => {},
  onGet: (_response: GetResponse) => {},
  onUpdate: (_up: UpdateResponse) => {},
  onUpdateNotification: (_notif: UpdateNotification) => {},
  onErr: (err: HostError) => {},
  onOpen: () => {},
};

const API_URL = new URL(`ws://${location.host}/contract/command/`);
const locutusApi = new LocutusWsApi(API_URL, handler);

const CONTRACT = "DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1";

async function loadState() {
  let getRequest = {
    key: Key.fromSpec(CONTRACT),
    fetch_contract: false,
  };
  await locutusApi.get(getRequest);
}
```

Let's unpack this code:

```typescript
const handler = {
  onPut: (_response: PutResponse) => {},
  onGet: (_response: GetResponse) => {},
  onUpdate: (_up: UpdateResponse) => {},
  onUpdateNotification: (_notif: UpdateNotification) => {},
  onErr: (err: HostError) => {},
  onOpen: () => {},
};

const API_URL = new URL(`ws://${location.host}/contract/command/`);
const locutusApi = new LocutusWsApi(API_URL, handler);
```

This type provides a convenient interface to the WebSocket API. It receives an
object which handles the different responses from the node via callbacks. Here
you would be able to interact with DOM objects or other parts of your code.

```typescript
const CONTRACT = "DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1";

async function loadState() {
  let getRequest = {
    key: Key.fromSpec(CONTRACT),
    fetch_contract: false,
  };
  await locutusApi.get(getRequest);
}
```

Here we use the API wrapper to make a get request (which requires a key and
specifies if we require fetching the contract code or not) to get the state for
a contract with the given address. The response from the node will be directed
to the `onGet` callback. You can use any other methods available in the API to
interact with the node.

<!--
TODO: Add a link to documentation for the WebSocket API in typescript
-->

## Writing the backend for our web application

In the [creating a new contract](dev-guide.md#creating-a-new-contract) section
we described the contract interface, but we were using it to write a simple
container contract that won't be doing anything in practice, just carrying
around the front end of your application. The core logic of the application, and
a back end where we will be storing all the information, requires another
contract. So we will create a new contract in a different directory for it:

```bash
$ cd ../backend
$ ldt new contract
```

This will create a regular contract, and we will need to implement the interface
on a type that will handle our contract code. For example:

```rust,noplayground
use locutus_stdlib::prelude::*;

pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];

struct Contract;

struct Posts(...)

impl Posts {
  fn add_post(&mut self, post: Post) { ... }
}

struct Post(...)

#[contract]
impl ContractInterface for Contract {
    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut posts: Posts = serde_json::from_slice(&state).map_err(|_| ContractError::InvalidState)?;
        if let Some(UpdateData::Delta(delta)) = data.pop() {
          let new_post: Posts = serde_json::from_slice(&delta).map_err(|_| ContractError::InvalidState);
          posts.add_post(new_post)?;
        } else {
            Err(ContractError::InvalidUpdate)
        }
        Ok(UpdateModification::valid(posts.into()))
    }

    ...
}
```

In this simple example, we convert a new incoming delta to a post and the state
to a list of posts we maintain, and we append the post to the list of posts.
After that, we convert back the posts list to an state and return that.

If we subscribe to the contract changes or our web app, we will receive a
notification with the updates after they are successful, and we will be able to
render them in our browser. We can do that, for example, using the API:

```typescript
function getUpdateNotification(notification: UpdateNotification) {
  let decoder = new TextDecoder("utf8");
  let updatesBox = DOCUMENT.getElementById("updates") as HTMLPreElement;
  let newUpdate = decoder.decode(Uint8Array.from(notification.update));
  let newUpdateJson = JSON.parse(newUpdate);
  updatesBox.textContent = updatesBox.textContent + newUpdateJson;
}
```

### Building and packaging a contract

Now that we have the front end and the back end of our web app, we can package
the contracts and run them in the node to test them out.

In order to do that, we can again use the development tool to help us out with
the process. But before doing that, let's take a look at the manifesto format
and understand the different parameters that allow us to specify how this
contract should be compiled (check the [manifest](./manifest.md) details for
more information). In the web app directory, we have a `locutus.toml` file which
contains something similar to:

```toml
[contract]
type = "webapp"
lang = "rust"

...

[webapp.state-sources]
source_dirs = ["dist"]
```

This means that the `dist` directory will be packaged as the initial state for
the webapp (that is the code the browser will be interpreting and in the end,
rendering).

If we add the following keys to the manifesto:

```toml
[webapp.dependencies]
posts = { path = "../backend" }
```

The WASM code from the `backend` contract will be embedded in our web
application state, so it will be accessible as a resource just via the local
HTTP gateway access and then we can re-use it for publishing additional
contracts.

<!--
TODO: Publishing to the real functioning Locutus network is not yet supported.
-->

Currently, wep applications follow a standarized build procedure in case you use
`ldt` and assumptions about your system. For example, in the case of a `type =
"webapp"` contract, if nothing is specified, it will assume you have `npm` and
the `tsc` compiler available at the directory level, as well as `webpack`
installed.

This means that you have installed either globally or at the directory level,
e.g. globally:

```
$ npm install -g typescript
$ npm install -g webpack
$ npm install -g webpack-cli
```

or locally (make sure your `package.json` file has the required dependencies):

```
$ npm install typescript --save-dev
$ npm install webpack --save-dev
$ npm install webpack-cli --save-dev
```

If, however, you prefer to follow a different workflow, you can write your own
by enabling/disabling certain parameters or using a blank template. For example:

```
[contract]
lang = "rust"

[state]
files = ["my_packaged_web.tar.xz"]
```

Would just delegate the work of building the packaged `tar` to the developer.
Or:

```
[contract]
type = "webapp"
lang = "rust"

[webapp]
lang = "typescript"

[webapp.typescript]
webpack =  false
```

would disable usign `webpack` at all.

Now that we understand the details, and after making any necessary changes, in
each contract directory we run the following commands:

```bash
$ ldt build
```

This command will read your contract manifest file (`locutus.toml`) and take
care of building the contract and packaging it, ready for the node and the
network to consume it.

<!--
TODO: Elsewhere in the documentation, explain the intricate details of building and deploying contracts, in case the use-case doesn't fit with the current tooling, so they know the necessary steeps to interact with the node at a lower level.
-->

Under the `./build/locutus` directory, you will see both a `*.wasm` file, which
is the contract file, and `contract-state`, in case it applies, which is the
initial state that will be uploaded when initially putting the contract.

Web applications can access the code of backend contracts directly in their
applications and put new contracts (that is, assigning a new location for the
code, plus any parameters that may be generated dynamically by the web app, and
the initial state for that combination of contract code + parameters)
dynamically.

Let's take a look at the manifest for our web app container contract:

## Testing out contracts in the local node

Once we have all our contracts sorted and ready for testing, we can do this in
local mode in our node. For this the node must be running, we can make sure that
is running by running the following command as a background process or in
another terminal; since we have installed it:

```bash
$ locutus-node
```

You should see some logs printed via the stdout of the process indicating that
the node HTTP gateway is running.

Once the HTTP gateway is running, we are ready to publish the contracts to our
local Locutus node:

```bash
$ cd ../backend && ldt publish --code="./build/locutus/backend.wasm" --state="./build/locutus/contract-state"
$ cd ../web && ldt publish --code="./build/locutus/web.wasm" --state="./build/locutus/contract-state"
```

In this case, we're not passing any parameters (so our parameters will be an
empty byte array), and we are passing an initial state without the current
backend contract. In typical use, both the parameters would have meaningful
data, and the backend contract may be dynamically generated from the app and
published from there.

Once this is done, you can start your app just by pointing to it in the browser:
`http://127.0.0.1:50509/contract/web/<CONTRACT KEY>`

For example
`http://127.0.0.1:50509/contract/web/CYXGxQGSmcd5xHRJNQygPwmUJsWS2njh3pdVjfVz9EV/`

Iteratively you can repeat this process of modifying, and publishing locally
until you are confident with the results and ready to publish your application.

Since the web is part of your state, you are always able to update it, pointing
to new contracts, and evolving it over time.

## Limitations

- Publishing to the Locutus network is not yet supported.
- Only Rust is currently supported for contract development, but we'll support
  more languages like [AssemblyScript](https://www.assemblyscript.org/) in the
  future.

- Binaries for all the required tools are not yet available, they must be
  compiled from source
