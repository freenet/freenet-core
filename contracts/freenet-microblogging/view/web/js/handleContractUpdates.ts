import { LocutusWsApi, PutResponse, GetResponse, UpdateResponse, UpdateNotification } from "stdlib";

const MODEL_CONTRACT = "JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC";

function getDocument(): Document {
    if (document) {
        return document;
    } else {
        throw new Error("document not present");
    }
}
const DOCUMENT: Document = getDocument();

let wsUri = ((window.location.protocol === "https:" && "wss://") || "ws://") +
    window.location.host +
    `/contract/dependency/${MODEL_CONTRACT}/changes/`;

function getState(hostResponse: GetResponse) { }

function getUpdate(update: UpdateNotification) { }

const HANDLER = {
    onPut: (_response: PutResponse) => { },
    onGet: getState,
    onUpdate: (_response: UpdateResponse) => { },
    onUpdateNotification: getUpdate,
    onErr: (_response: Error) => { },
}

const API_URL = new URL("");
let locutusApi = new LocutusWsApi(API_URL, HANDLER);

function sendUpdate() {
    let input = DOCUMENT.getElementById("input") as null | HTMLTextAreaElement;
    let sendVal: HTMLTextAreaElement;
    if (!input) {
        throw new Error();
    } else {
        sendVal = input;
    }

    let updateRequest = {
        key: new Key(""),
        delta: new Uint8Array(sendVal.value)
    }
    locutusApi.update(updateRequest);
}

function registerUpdater() {
    let updateBtn = DOCUMENT.getElementById("btn");
    if (!updateBtn)
        throw new Error();
    else
        updateBtn.addEventListener("click", sendUpdate);
}
registerUpdater();


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
