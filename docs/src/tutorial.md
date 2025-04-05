# Freenet Decentralized App Tutorial

This tutorial walks you through creating and running a simple web app and a backend contract on Freenet. By the end, you’ll have:

- A **web application** (frontend) that runs in the browser and talks to your local Freenet node.
- A **container contract** that holds and distributes your web application over Freenet.
- A **backend contract** that stores data and provides any server-like logic.

Use the accompanying example ([freenet-email-app](https://github.com/freenet/freenet-core/tree/main/apps/freenet-email-app)) for reference.

---

## 1. Prerequisites

### 1.1 Rust & Cargo

Install Rust and Cargo (Linux/macOS example):

```bash
curl https://sh.rustup.rs -sSf | sh
```

> **Important for macOS:** Using Homebrew’s Rust can interfere with `fdev`. Use `rustup` above instead.

### 1.2 Freenet Core & FDev (Git Installation)

Freenet Core is under rapid development, so install from Git:

```bash
git clone --recurse-submodules https://github.com/freenet/freenet-core.git
cd freenet-core/apps/freenet-ping
```

Then install the Freenet kernel (`freenet`) and dev tool (`fdev`):

```bash
cargo install --path ../../crates/core
cargo install --path ../../crates/fdev
```

### 1.3 WebAssembly Target

Enable the WebAssembly target for Rust:

```bash
rustup target add wasm32-unknown-unknown
```

### 1.4 Node.js & TypeScript

If you plan to build web UIs using TypeScript:

- **Ubuntu**:
  ```bash
  sudo apt update
  sudo apt install nodejs npm
  ```
- **macOS/Windows**: Install from [Node.js downloads](https://nodejs.org/en/download/).

Then install TypeScript globally:

```bash
sudo npm install -g typescript
```

Check:

```bash
tsc --version
```

---

## 2. Create Your Project Structure

We’ll create two main pieces: a **web app** (which is essentially our frontend) and a **backend contract**.

### 2.1 Create a Web App (Container + Frontend)

```bash
mkdir -p my-app/web
mkdir -p my-app/backend
cd my-app/web
fdev new web-app
```

This makes a skeleton for:

1. A **container contract** in `./container/` (stores and distributes your web files).
2. A TypeScript-based web app in `./src/`.

### 2.2 Container Contract Overview

Open `my-app/web/container/src/lib.rs`. You’ll see something like:

```rust
use freenet_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    // ...
}
```

- `#[contract]` links your code to the Freenet WASM runtime.

**Tip**: Real container contracts typically add checks (e.g., who can update the contract).

---

## 3. Frontend (Web Application) Basics

Your web files live under `my-app/web/src/`. A typical TypeScript setup might use `npm install` to pull in `@freenetorg/freenet-stdlib`. For example in `package.json`:

```json
{
  "dependencies": {
    "@freenetorg/freenet-stdlib": "0.0.6"
  }
}
```

Then in `src/index.ts`, you can import `@freenetorg/freenet-stdlib` and talk to the node:

```ts
import {
  FreenetWsApi,
  GetResponse,
  HostError,
  Key,
  PutResponse,
  UpdateResponse,
  // ...
} from "@freenetorg/freenet-stdlib/websocket-interface";

const handler = {
  onContractPut: (_: PutResponse) => {},
  onContractGet: (_: GetResponse) => {},
  onContractUpdate: (_: UpdateResponse) => {},
  // ...
  onErr: (err: HostError) => console.error(err),
  onOpen: () => console.log("WebSocket open!"),
};

const API_URL = new URL(`ws://${location.host}/contract/command/`);
const freenetApi = new FreenetWsApi(API_URL, handler);

// Example contract key
const CONTRACT_KEY = "DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1";

async function loadState() {
  await freenetApi.get({
    key: Key.fromSpec(CONTRACT_KEY),
    fetch_contract: false,
  });
}
```

---

## 4. Backend Contract

Switch to your backend folder and scaffold a regular contract:

```bash
cd ../backend
fdev new contract
```

Edit `./src/lib.rs`. Here’s a skeleton that stores a list of posts:

```rust
use freenet_stdlib::prelude::*;

struct Contract;

struct Posts(Vec<Post>);

#[derive(Serialize, Deserialize)]
struct Post {
  // ...
}

#[contract]
impl ContractInterface for Contract {
    fn update_state(
        _params: Parameters<'static>,
        current_state: State<'static>,
        mut data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        // Convert the current state from JSON
        let mut posts: Posts = serde_json::from_slice(&current_state)
          .map_err(|_| ContractError::InvalidState)?;

        if let Some(UpdateData::Delta(delta)) = data.pop() {
            // Parse the incoming update as a Post
            let new_post: Post = serde_json::from_slice(&delta)
              .map_err(|_| ContractError::InvalidUpdate)?;

            // Append to our existing posts
            posts.0.push(new_post);
        } else {
            return Err(ContractError::InvalidUpdate);
        }

        // Serialize back to JSON for the updated state
        Ok(UpdateModification::valid(serde_json::to_vec(&posts)
          .unwrap()
          .into()))
    }

    // ...
}
```

Any time the contract receives an update, it takes the JSON-encoded “post” and appends it to the stored list. The front end can subscribe to these updates and reflect them in the UI.

---

## 5. Building & Packaging

### 5.1 Manifest Configuration

Each contract folder has a `freenet.toml` specifying how to build/package. In the **web container** (`my-app/web`), you might see:

```toml
[contract]
type = "webapp"
lang = "rust"

[webapp.state-sources]
source_dirs = ["dist"]  # The compiled web files get packaged from this folder
```

You can embed your backend contract into the container by adding something like:

```toml
[webapp.dependencies]
posts = { path = "../backend" }
```

### 5.2 Compile

From inside `my-app/web`:

```bash
fdev build
```

This runs any necessary steps (like `npm install`, `webpack`, TypeScript compilation) before creating a `.wasm` file and `contract-state` in `build/freenet`.

Then do the same in `my-app/backend`:

```bash
fdev build
```

You should again see a `.wasm` file and optionally a `contract-state`.

---

## 6. Test Locally

### 6.1 Start the Local Freenet Node

In one terminal:

```bash
freenet
```

You’ll see logs indicating the node is running (HTTP gateway on `127.0.0.1:50509` by default).

### 6.2 Publish Contracts

In another terminal, publish both contracts:

**Backend**:

```bash
cd my-app/backend
fdev publish --code="./build/freenet/backend.wasm" --state="./build/freenet/contract-state"
```

**Web Container**:

```bash
cd ../web
fdev publish --code="./build/freenet/web.wasm" --state="./build/freenet/contract-state"
```

### 6.3 View in Browser

You’ll get a contract key when you publish (e.g., `CYXGxQGSmcd5xHRJNQygPwmUJsWS2njh3pdVjfVz9EV`).  
Open it in your browser:

```
http://127.0.0.1:50509/contract/web/<CONTRACT-KEY>
```

If everything went well, you’ll see your web application loaded from the node.

---

## 7. Updating Your App

Since the app’s code is stored as the contract’s **state**, you can update it at any time by:

1. Changing your TypeScript/Rust source.
2. Rebuilding.
3. Publishing a new version with a new state or new parameters.

The Freenet node treats these as regular contract updates.

---

## 8. Current Limitations

- **No Real Network**: Publishing to the production Freenet network is still under development.
- **Language Support**: Currently only Rust for contracts; additional languages (e.g., AssemblyScript) planned.
- **Manual Install**: Binaries for `fdev` and `freenet` require manual build from source.

---

**That’s it!** You’ve built a basic decentralized web app on Freenet using a container contract (for the UI) and a backend contract (for the data/logic). For a more complete example, check the [freenet-email-app](https://github.com/freenet/freenet-core/tree/main/apps/freenet-email-app).
