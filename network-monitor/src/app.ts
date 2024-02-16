import * as flatbuffers from "flatbuffers";
import * as fbTopology from "./generated/topology";
import { handleChange } from "./topology";

const PEER_CHANGES = new WebSocket(
    "ws://127.0.0.1:55010/pull-stats/peer-changes/"
);
PEER_CHANGES.onmessage = handlePeerChanges;

// const DELIVER_MESSAGE = new WebSocket("ws://127.0.0.1:55010/pull-stats/network-events/");

function handlePeerChanges(event: MessageEvent) {
    const data = event.data as Blob;
    convertBlobToUint8Array(data)
        .then((uint8Array) => {
            const buf = new flatbuffers.ByteBuffer(uint8Array);
            const peerChange = fbTopology.PeerChange.getRootAsPeerChange(buf);
            handleChange(peerChange);
        })
        .catch((error) => {
            console.error("Failed to handle message:", error);
        });
}

function convertBlobToUint8Array(blob: Blob): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onloadend = () => {
            if (reader.result instanceof ArrayBuffer) {
                const arrayBuffer = reader.result;
                const uint8Array = new Uint8Array(arrayBuffer);
                resolve(uint8Array);
            } else {
                reject(new Error("Failed to convert Blob to Uint8Array."));
            }
        };
        reader.onerror = reject;
        reader.readAsArrayBuffer(blob);
    });
}
