import {
    GetRequest,
    GetResponse,
    HostError,
    ContractKey,
    FreenetWsApi,
    PutResponse,
    UpdateNotification,
    UpdateResponse,
    UpdateRequest,
    SubscribeRequest,
    UpdateData,
    DeltaUpdate,
    DelegateResponse,
} from "@freenetorg/freenet-stdlib";
import {UpdateDataType} from "@freenetorg/freenet-stdlib/common";

import "./scss/styles.scss";

// import * as bootstrap from "bootstrap";

function getDocument(): Document {
    if (document) {
        return document;
    } else {
        throw new Error("document not present");
    }
}

const DOCUMENT: Document = getDocument();


const MODEL_CONTRACT = `${process.env.MODEL_CONTRACT}`;
const KEY = ContractKey.fromInstanceId(MODEL_CONTRACT);

function getState(hostResponse: GetResponse) {
    let decoder = new TextDecoder("utf8");
    let currentStateBox = DOCUMENT.getElementById(
        "current-state"
    ) as HTMLPreElement;
    let state = decoder.decode(Uint8Array.from(hostResponse.state));
    currentStateBox.textContent = JSON.stringify(
        JSON.parse(state),
        ["messages", "author", "date", "title", "content"],
        2
    );
}

function getUpdateNotification(notification: UpdateNotification) {
    let decoder = new TextDecoder("utf8");
    let updatesBox = DOCUMENT.getElementById("updates") as HTMLPreElement;
    let delta = notification.update?.updateData as DeltaUpdate;
    let newUpdate = decoder.decode(Uint8Array.from(delta.delta));
    let newUpdateJson = JSON.parse(newUpdate.replace("\x00", ""));
    let newContent = JSON.stringify(
        newUpdateJson,
        ["author", "title", "content", "mod_msg", "signature"],
        2
    );

    updatesBox.textContent = updatesBox.textContent + newContent;
}

async function sendUpdate() {
    let input = DOCUMENT.getElementById("input") as null | HTMLTextAreaElement;
    let sendVal: HTMLTextAreaElement;
    if (!input) {
        throw new Error();
    } else {
        sendVal = input;
    }

    if (isValidUpdate(sendVal.value)) {
        let encoder = new TextEncoder();
        const delta = new DeltaUpdate(
            Array.from(encoder.encode("[" + sendVal.value + "]"))
        );
        const update = new UpdateData(UpdateDataType.DeltaUpdate, delta);
        let updateRequest = new UpdateRequest(KEY, update);
        await freenetApi.update(updateRequest);
    }
}

function isValidUpdate(input: string): boolean {
    const expectedKeys = new Set(["author", "date", "title", "content"]);
    try {
        let inputJson = JSON.parse(input);

        if (Array.isArray(inputJson)) {
            return false;
        }

        let keys_set = new Set(Object.keys(inputJson));
        if (keys_set.size !== expectedKeys.size) {
            alert("The input json does not contain the expected keys");
            return false;
        }

        for (let key of expectedKeys) {
            if (!keys_set.has(key)) {
                alert("The input key" + key + "does not exist");
                return false;
            }
        }

        return true;
    } catch (e) {
        alert("Invalid json: " + input);
        return false;
    }
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
    const subscribe_request: SubscribeRequest = new SubscribeRequest(
        KEY,
        new Array<number>()
    );
    await freenetApi.subscribe(subscribe_request);
}

const handler = {
    onContractPut: (_response: PutResponse) => {
    },
    onContractGet: getState,
    onContractUpdate: (_up: UpdateResponse) => {
    },
    onContractUpdateNotification: getUpdateNotification,
    onDelegateResponse: (_response: DelegateResponse) => {
    },
    onErr: (err: HostError) => {
        console.log("Received error, cause: " + err.cause);
    },
    onOpen: () => {
        registerUpdater();
        registerGetter();
        subscribeToUpdates();
    },
};

const API_URL = new URL(`ws://${location.host}/contract/command`);
const freenetApi = new FreenetWsApi(API_URL, handler);

async function loadState() {
    const key = ContractKey.fromInstanceId(MODEL_CONTRACT);
    const fetchContract = false;
    const getRequest: GetRequest = new GetRequest(key, fetchContract);

    await freenetApi.get(getRequest);
}

window.addEventListener("load", function (_ev: Event) {
    loadState();
});
