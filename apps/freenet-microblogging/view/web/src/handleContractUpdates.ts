import { LocutusWsApi, PutResponse, GetResponse, UpdateResponse, UpdateNotification, Key, HostError } from "locutus-stdlib/src/webSocketInterface";

function getDocument(): Document {
    if (document) {
        return document;
    } else {
        throw new Error("document not present");
    }
}
const DOCUMENT: Document = getDocument();

const MODEL_CONTRACT = "6Q2zMtcHwsUyWg5VaR15Xn2yoNxjzufTJsSUHuajEijG";

function getState(hostResponse: GetResponse) {
    console.log("Received get");
    let decoder = new TextDecoder();
    let state_content = JSON.parse(decoder.decode(hostResponse.state));
    let text_box = DOCUMENT.getElementById("input") as HTMLTextAreaElement;
    text_box.textContent = JSON.stringify(state_content, null, 2)
}

function getUpdate(update: UpdateResponse) {
    let decoder = new TextDecoder();
    let decoded_summary = decoder.decode(new Uint8Array(update.summary));
    decoded_summary = decoded_summary.replaceAll('\x00', '');
    let summaries = JSON.parse(decoded_summary)['summaries'];
    summaries.forEach(function (update: Uint8Array) {
        let decoded_update = decoder.decode(update);
        let update_content = JSON.parse(decoded_update);
        console.log("Received update: " + JSON.stringify(update_content, null, 2));
        let updates_box = DOCUMENT.getElementById("updates") as HTMLPreElement;
        updates_box.textContent =
            updates_box.textContent + "\n" + JSON.stringify(update_content, null, 2);
    });
}

const HANDLER = {
    onPut: (_response: PutResponse) => { },
    onGet: getState,
    onUpdate: getUpdate,
    onUpdateNotification: (_response: UpdateNotification) => {},
    onErr: (_response: HostError) => { },
}

const API_URL = new URL(`ws://${location.host}/contract/command/`);
let locutusApi = new LocutusWsApi(API_URL, HANDLER);

async function loadState() {
    let getRequest = {
        key: Key.fromSpec(MODEL_CONTRACT),
        fetch_contract: false
    };
    await locutusApi.get(getRequest);
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
        key: Key.fromSpec(MODEL_CONTRACT),
        delta: encoder.encode(sendVal.value)
    };
    await locutusApi.update(updateRequest);
}

function registerUpdater() {
    let updateBtn = DOCUMENT.getElementById("su-btn");
    if (!updateBtn)
        throw new Error();
    else
        updateBtn.addEventListener("click", sendUpdate);
}

function registerGetter() {
    let getBtn = DOCUMENT.getElementById("ls-btn");
    if (!getBtn)
        throw new Error();
    else
        getBtn.addEventListener("click", loadState);
}

async function subscribeToUpdates() {
    console.log("subscribing to contract:" + MODEL_CONTRACT);
    await locutusApi.subscribe({
        key: Key.fromSpec(MODEL_CONTRACT)
    });
    console.log("sent subscription request");
}

// Connection opened
locutusApi.ws.addEventListener('open', (_) => {
    registerUpdater();
    registerGetter();
    subscribeToUpdates();
});
