# Development Guide

This guide will walkthrough how to develop a simple distributed web application using Locutus. To do that we will be using Rust for the contracts themselves and Typescript for developing the web application.

## Installation

Development for Locutus requires setting initial dependencies. Currently the following dependencies are necessary:

- A [Rust](https://www.rust-lang.org/tools/install) installation, along with [Cargo](https://doc.rust-lang.org/cargo/), the Rust package manager.

      🛈 Currently contract development is only supported in Rust.

- Locutus development tools and node.

      🛈 In the future we will distribute binaries for all the required tools.

- The [LLVM](https://llvm.org) compiler backend core libraries. Usually available at most OS package managers for Linux distributions and Mac OS.

Once you have a working installation of Cargo you can install the dev tools:

```
$ cargo install locutus
```

This command will install `ldt` (Locutus Dev Tool) and a working node that can be used for local development.

You can find more information about the available commands looking at [ldt](ldt.md) information or by executing lbt with the `--help` argument.

## Creating a new contract

You can create a new [contract](glossary.md#contract) skeleton by executing the `new` command with `lbt`. Two contract types are supported currently by the tool, regular [contracts](glossary.md#contract), and [web application](glossary.md#web-application) [container contracts](glossary.md#container-contract).

Currently the following technological stacks are supported (more to be added in the future):

- Regular contracts:
  - Rust (_default_)
- Web applications:
  - Container development:
    - Rust (_default_)
  - Web/state development:
    - Typescript. (_default: using npm and webpack_)
    - Javascript.
    - Rust (**WIP**).

We will need to create a directory which will hold our web app and initialize it:

```
$ mkdir -p my-app/web
$ mkdir -p my-app/backend
$ cd my-app/web
$ ldt new web-app
```

will create the skeleton for a web application and its container contract for Locutus ready for development at the `my-app/web` directory.

## Making a container contract

In order to create a contract, the first thing that we need is to write the code for our container contract. This is a shell contract that will be deployed in the network in order to access the content of our web application.

The `new` command has created the source ready to be modified for us, in your favourite editor open the following file:

```
$ ./container/src/lib.rs
```

In this case, and for simplicity sake, the contract won't be performing any functions, but in a real contract this contract would be including some basic security functionality, e.g. verifiying that whoever is trying to update the contract has the required credentials.

> **TODO:** Point to included stdlib functionality to generate safe contract containers.

In order to make our contract unique so it doesn't collide with an existing contract, we can generate a random signature that will be embeeded with the contract.

      🛈 What would happen in case of collision with an existing contract? (That would be if we try to publish a contract which has the same exact combination of code and parameters.) Then it would fail to publish our contract in the network and would get a rejection because we would be trying to update an existing contract. And we would have to make a slight change in the code/parameters so this collision is avoided.

In order to make this work, there needs to exist a type, which requires (this can be only done once, at the top level of the library crate) implementing the `ContracInterface` trait from the locutus-stdlib. For example in the `lib.rs` file we will write the following:

```rust,noplayground
use locutus_stdlib::prelude::*;

pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(_parameters: Parameters<'static>, _state: State<'static>) -> bool {
        true
    }

    fn validate_delta(_parameters: Parameters<'static>, _delta: StateDelta<'static>) -> bool {
        true
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        Ok(UpdateModification::ValidUpdate(state))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
    ) -> StateSummary<'static> {
        StateSummary::from(vec![])
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        StateDelta::from(vec![])
    }
}
```

That's a lot of information, let's unpack it:

```rust,noplayground
use locutus_stdlib::prelude::*;
```

Here we are importing the necessary types and traits to write a Locutus contract successfully using Rust.

```rust,noplayground
pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];
```

This will make our contract unique, notice the `pub` qualifier so the compiler doesn't remove this constant because is unused and is included in the output of the compiler.

```rust,noplayground
struct Contract;

#[contract]
impl ContractInterface for Contract {
  ...
}
```

      🛈 The exact interface still is an evolving specification.

> **TODO:** Elsewhere in the documentation, explain the intricate details of how interfacing through WASM works. In theory users could implement their own wrapping code as long as the follow the low level WASM code specification.

Here we create a new type, `Contract` for which we will be implementing the `ContractInterface` trait. To know more details about the functionality of a contract, delve into the details of the [contract interface](contract-interface.md).

Notice the `#[contract]` macro call, this will generate the necessary code for the WASM runtime to interact with your contract in an ergonomic ans safe way. Trying to use this macro more than once in the same module will result in a compiler error, and only the code generated at the top level module will be used by the runtime.

As a rule of thumb, one contract will require to implement the `ContractInterface` exactly once.

### Creating a web application

Now we have a working example of a contract, but our contract is an empty shell, which does not do anything yet. To change this, we will start developing our web application.

In order to do that, we can go and modify the code of the contract state, which in this case is the web application. Locutus offers a std library which can be used with Typescript/Javascript to facilitate the development of web applications and interfacing with your local node, so we will make our `package.json` contains the dependency:

```
{
  "dependencies": {
    "@locutus/locutus-stdlib": "0.0.2"
  }
}
```

Open the file `src/index.ts` in a code editor and you can start developing the web application.

An important thing to notice is that our application will have be interfacing with our local node, the entry point for our machine to communicate with other nodes in the network. The stdlib offers a series of facilities in which you will be able to communicate with the network ergonomically.

Here is an example on how you could write your application to interact with the node:

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

This type wraps the node websocket API, and allows communicating with. It receives an object which handles the different responses from the node via callbacks. Here you would be able to interact with DOM objects or other parts of your code.

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

Here we use the API wrapper to make a get request (which requires a key and specifying if we require fetching the contract code or not) to get the state for a contract with the given address. The response from the node will be directed to the `onGet` callback. You can use any other methods available in the API to interact with the node.

> **TODO:** Add a link to documentation for the websocket API in typescript

## Writing the backend for our web application

On the [creating a new contract](dev-guide.md#creating-a-new-contract) section we described the contract interface, but we were using to write a simple container contract which won't be doing nothing in practice, just carrying around the frontend of your application. The core logic of the application, and backend where we will be storing all the information, requires an other contract. So we will create a new contract in a different directory for it:

```
$ cd ../backend
$ ldt new contract
```

This will create a regular contract, and we will need to implement the interface on a type that will handle our contract code. For example:

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
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let mut posts: Posts = serde_json::from_slice(&state).map_err(|_| ContractError::InvalidState)?;
        let new_post: Posts = serde_json::from_slice(&delta).map_err(|_| ContractError::InvalidState);
        posts.add_post(new_post)?;
        Ok(UpdateModification::ValidUpdate(state))
    }

    ...
}
```

In this simple example, we convert a new incoming delta to a post, and the state to a list of posts we maintain, and we appen the post to the list of posts.

If we subscribe to the contract changes or our web app, we will receive a notification with the updates after they are successful, and we will be able to render them in our browser. We can do that, for example, using the API:

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

Now that we have the frontend and the backend of our web app, we can package the contracts and run them in the node to test them out.

In order to do that, we can again use the development tool to help us out with the process, in each contract directory we run the following commands:

```
$ ldt build
```

This command will read your contract manifest file (`locutus.toml`) and take care of building the contract and packaging it, ready for the node and the network to consume it.

> **TODO:** Elsewhere in the documentation, explain the intricate details of building and deploying contracts, in case the usecase doesn't fit with the current tooling, so they know the necessary steeps to itneract with the node at a lower level.

Under the `./build/locutus` directory you will see both a `*.wasm` file, which is the contract file, and `contract-state`, in case it applies, which is the initial state that will be uploaded when initially putting the contract.

Web applications can access the code of backend contracts directly in their applications and put new contracts (that is, assigning a new location for the code, plus any parameters that may be generated dinamically by the web app, and the initial state for that combination of contract code + parameters) dinamically.

Let's take a look at the manifest for our web app container contract:

```
[contract]
type = "webapp"
lang = "rust"

...

[webapp.state-sources]
source_dirs = ["dist"]
```

This means that the + dist` directory will be packaged as the initial state for the webapp (that is the code the browser will be interpreting and in the end, rendering).

If we add the following keys to the manifesto:

```
[webapp.dependencies]
posts = { path = "../backend" }
```

The WASM code from the `backend` contract will be embedded in our web application state, so it will be accesible as a resource just via the local HTTP gateway access and then we can re-use it for publishing additional contracts.

> **TODO:** Publishing to the real functioning Locutus network is not yet supported.

## Testing out contracts in the local node

Once we have all our contracts sorted and ready for testing, we can do this in local mode in our node. For this the node must be running, we can make sure that is running by running the following command as a background process or in other terminal, since we have installed it:

```
$ locutus-node
```

You should see some logs printed via the stdout of the process indicating that the node HTTP gateway is running.

Once the HTTP gateway is running, we are ready to put the contracts in the node:

```
$ cd ../backend && ldt publish --code="./build/locutus/backend.wasm" --state="./build/locutus/contract-state"
$ cd ../web && ldt publish --code="./build/locutus/web.wasm" --state="./build/locutus/contract-state"
```

In this case we are not passing any parameters (so ours parameters will be basically an empty byte array), and we are passing an initial state with out current backend contract. In a typical use both the parameters would have meaningful data, and the backend contract may be dinamically egenrated from the app and published from there. But the main idea is that how you would publish your application.

Once this is done, you can browse your web just pointing to it in the browser: `http://127.0.0.1:50509/contract/web/<CONTRACT KEY>/`

For example: `http://127.0.0.1:50509/contract/web/CYXGxQGSmcd5xHRJNQygPwmUJsWS2njh3pdVjfVz9EV/`

Iteratively you can repeat this process of modifying, publishing locally, until you are confident with the results and ready to publish your application.

Since the web is part of your state, you are always able to update it, pointing to new contracts, and evolve it over time.

## Publishing

> **TODO:** Publishing to the real functioning Locutus network is not yet supported.
