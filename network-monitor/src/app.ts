import * as flatbuffers from "flatbuffers";
import * as fbTopology from "./generated/topology";
import { handleChange } from "./topology";
import { handlePutRequest, handlePutSuccess } from "./transactions-data";
import { parse_put_msg_data } from "./utils";

let connection_established = false;

const ws_connection_interval = setInterval(() => {
    if (!connection_established) {
        try {
            const socket = new WebSocket(
                "ws://127.0.0.1:55010/pull-stats/peer-changes/"
            );

            socket.addEventListener("open", () => {
                connection_established = true;
                console.log("WS Connection established");
            });

            socket.addEventListener("message", handleChanges);
        } catch (e) {
            console.error(e);
        }
    } else {
        console.log("WS Connection established");
        clearInterval(ws_connection_interval);
    }
}, 3000);

// const DELIVER_MESSAGE = new WebSocket("ws://127.0.0.1:55010/pull-stats/network-events/");

function handleChanges(event: MessageEvent) {
    const data = event.data as Blob;
    convertBlobToUint8Array(data)
        .then((uint8Array) => {
            const buf = new flatbuffers.ByteBuffer(uint8Array);

            try {
                const contractChange =
                    fbTopology.ContractChange.getRootAsContractChange(buf);

                console.log(contractChange.changeType());

                if (
                    contractChange.changeType() ===
                    fbTopology.ContractChangeType.PutRequest
                ) {
                    let {
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type,
                    } = parse_put_msg_data(
                        contractChange,
                        fbTopology.ContractChangeType.PutRequest
                    );

                    handlePutRequest(
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type
                    );

                    return;
                }

                if (
                    contractChange.changeType() ===
                    fbTopology.ContractChangeType.PutSuccess
                ) {
                    let {
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type,
                    } = parse_put_msg_data(
                        contractChange,
                        fbTopology.ContractChangeType.PutSuccess
                    );

                    handlePutSuccess(
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type
                    );

                    return;
                }

                if (
                    contractChange.changeType() ===
                    fbTopology.ContractChangeType.PutFailure
                ) {
                    let {
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type,
                    } = parse_put_msg_data(
                        contractChange,
                        fbTopology.ContractChangeType.PutFailure
                    );

                    handlePutSuccess(
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type
                    );

                    return;
                }
            } catch (e) {
                console.error(e);
            }

            try {
                const peerChange =
                    fbTopology.PeerChange.getRootAsPeerChange(buf);
                handleChange(peerChange);

                return;
            } catch (e) {}
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
