# Locutus Javascript/Typescript Std Lib

Locutus Javascript/Typescript Standard Library contains utilities for developing web apps which interact with Locutus node.

The stdlib offers a series of facilities in which you will be able to communicate with the network ergonomically.

Here is a minimal example of how you could write your application to interact with the node:

```typescript
import {
  GetResponse,
  HostError,
  Key,
  LocutusWsApi,
  PutResponse,
  UpdateNotification,
  UpdateResponse,
} from "@locutus/locutus-stdlib/webSocketInterface";

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
