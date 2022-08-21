import {
  LocutusWsApi,
  PutResponse,
  GetResponse,
  UpdateResponse,
  UpdateNotification,
  Key,
  HostError,
} from "locutus-stdlib/src/webSocketInterface";
import "../src/scss/styles.scss";
// import * as bootstrap from 'bootstrap'

function getDocument(): Document {
  if (document) {
    return document;
  } else {
    throw new Error("document not present");
  }
}
const DOCUMENT: Document = getDocument();

const MODEL_CONTRACT = "6Q2zMtcHwsUyWg5VaR15Xn2yoNxjzufTJsSUHuajEijG";
const KEY = Key.fromSpec(MODEL_CONTRACT);

function getState(hostResponse: GetResponse) {
  console.log("Received get");
  let decoder = new TextDecoder("utf8");
  let currentStateBox = DOCUMENT.getElementById(
    "current-state"
  ) as HTMLTextAreaElement;
  currentStateBox.textContent = decoder.decode(
    Uint8Array.from(hostResponse.state)
  );
}

function getUpdateNotification(notification: UpdateNotification) {
  let decoder = new TextDecoder("utf8");
  let updatesBox = DOCUMENT.getElementById("updates") as HTMLPreElement;
  updatesBox.textContent =
    updatesBox.textContent +
    "\n" +
    decoder.decode(Uint8Array.from(notification.update));
}

async function sendUpdate() {
  let input = DOCUMENT.getElementById("input") as null | HTMLTextAreaElement;
  let sendVal: HTMLTextAreaElement;
  if (!input) {
    throw new Error();
  } else {
    sendVal = input;
  }

  let encoder = new TextEncoder();
  let updateRequest = {
    key: KEY,
    delta: encoder.encode("[" + sendVal.value + "]"),
  };
  await locutusApi.update(updateRequest);
}

function registerUpdater() {
  let updateBtn = DOCUMENT.getElementById("su-btn");
  if (!updateBtn) throw new Error();
  else updateBtn.addEventListener("click", sendUpdate);
}

function registerGetter() {
  let getBtn = DOCUMENT.getElementById("ls-btn");
  if (!getBtn) throw new Error();
  else getBtn.addEventListener("click", loadState);
}

async function subscribeToUpdates() {
  console.log(`subscribing to contract: ${MODEL_CONTRACT}`);
  await locutusApi.subscribe({
    key: KEY,
  });
  console.log(`sent subscription request to key: '${KEY.encode()}'`);
}

const handler = {
  onPut: (_response: PutResponse) => {},
  onGet: getState,
  onUpdate: (_up: UpdateResponse) => {},
  onUpdateNotification: getUpdateNotification,
  onErr: (err: HostError) => {
    console.log("Received error, cause: " + err.cause);
  },
  onOpen: () => {
    registerUpdater();
    registerGetter();
    subscribeToUpdates();
  },
};

const API_URL = new URL(`ws://${location.host}/contract/command/`);
const locutusApi = new LocutusWsApi(API_URL, handler);

async function loadState() {
  let getRequest = {
    key: Key.fromSpec(MODEL_CONTRACT),
    fetch_contract: false,
  };
  await locutusApi.get(getRequest);
}

window.addEventListener("load", function (_ev: Event) {
  loadState();
});
