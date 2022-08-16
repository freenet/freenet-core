import { LocutusWsApi, PutResponse, GetResponse, UpdateResponse, UpdateNotification, Key, HostError } from "locutus-stdlib/src/webSocketInterface";

function getDocument(): Document {
    if (document) {
        return document;
    } else {
        throw new Error("document not present");
    }
}
const DOCUMENT: Document = getDocument();

const MODEL_CONTRACT = "3kF9dWtuTmV33MSA7sjPW86reeevB1X8CdZ7rzvmWmNB";

function getState(hostResponse: GetResponse) {
    console.log("Received get");
    let decoder = new TextDecoder();
    let state_content = JSON.parse(decoder.decode(hostResponse.state));
    let text_box = DOCUMENT.getElementById("input") as HTMLTextAreaElement;
    text_box.textContent = JSON.stringify(state_content, null, 2)
}

function getUpdate(update: UpdateNotification) {
    console.log("Received update");
    let decoder = new TextDecoder();
    let update_content = JSON.parse(decoder.decode(update.update));
    let updates_box = DOCUMENT.getElementById("updates") as HTMLPreElement;
    updates_box.textContent =
        updates_box.textContent + "\n" + JSON.stringify(update_content, null, 2);
}

const HANDLER = {
    onPut: (_response: PutResponse) => { },
    onGet: getState,
    onUpdate: (_response: UpdateResponse) => { },
    onUpdateNotification: getUpdate,
    onErr: (_response: HostError) => { },
}

const API_URL = new URL(`ws://${location.host}/contract/command/`);
let locutusApi = new LocutusWsApi(API_URL, HANDLER);

function loadState() {
    let getRequest = {
        key: Key.fromSpec(MODEL_CONTRACT),
        fetch_contract: false
    }
    locutusApi.get(getRequest);
}

function sendUpdate() {
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
    }
    locutusApi.update(updateRequest);
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

registerUpdater();
registerGetter();


/*
<script>
    // this script should be ported to typescript and compiled+bundled with the web
    
    const reader = new FileReader();
    (response) => {
        reader.onload = () => {
            log("Received update: " + reader.result);
            let val = JSON.parse(reader.result);
            val = val.map((e) => {
                delete e.signature;
                return e;
            });
            document.getElementById("updates").textContent =
                JSON.stringify(val, null, 2);
        };
        reader.readAsText(e.data);
    }
</script>
*/
