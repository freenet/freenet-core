import * as flatbuffers from "flatbuffers";
import * as fbTopology from "./generated/topology";
import { handleChange } from "./topology";
import {
    handleBroadcastEmitted,
    handleBroadcastReceived,
    handlePutRequest,
    handlePutSuccess,
} from "./transactions-data";
import {
    get_change_type,
    parse_broadcast_emitted_msg,
    parse_broadcast_received_msg,
    parse_put_msg_data,
} from "./utils";
import { ChangeType } from "./type_definitions";
import { unionToContractChangeType } from "./generated/topology/contract-change-type";

let connection_established = false;

const ws_connection_interval = setInterval(() => {
    if (!connection_established) {
        try {
            console.log("Attempting to establish WS Connection");

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

            let errors = [];

            try {
                const contractChange =
                    fbTopology.ContractChange.getRootAsContractChange(buf);

                // console.log(
                //     "raw contract change changeType",
                //     contractChange.changeType()
                // );

                // console.log(
                //     "parsed contract change changeType",
                //     get_change_type(contractChange.changeType())
                // );

                let now_change_type = get_change_type(
                    contractChange.changeType()
                );

                if (now_change_type == ChangeType.BROADCAST_EMITTED) {
                    // console.log("processing BroadcastEmitted");

                    let {
                        transaction,
                        upstream,
                        broadcast_to,
                        key,
                        requester: sender,
                        timestamp,
                        contract_location,
                    } = parse_broadcast_emitted_msg(
                        contractChange,
                        contractChange.changeType()
                    );

                    handleBroadcastEmitted(
                        transaction,
                        upstream,
                        broadcast_to,
                        key,
                        sender,
                        timestamp,
                        contract_location
                    );

                    return;
                }

                if (now_change_type == ChangeType.BROADCAST_RECEIVED) {
                    let {
                        transaction,
                        target,
                        requester,
                        key,
                        change_type,
                        timestamp,
                        contract_location,
                    } = parse_broadcast_received_msg(
                        contractChange,
                        contractChange.changeType()
                    );

                    let fixed_target = target.split(" (@")[0];

                    let broadcast_target_peer_location = target
                        .split(" (@")[1]
                        .split(")")[0];

                    // console.log("original target", target);

                    // console.log("fixed target peer id", fixed_target);

                    // console.log(
                    //     "broadcast target peer location",
                    //     broadcast_target_peer_location
                    // );

                    handleBroadcastReceived(
                        transaction,
                        fixed_target,
                        requester,
                        key,
                        change_type,
                        timestamp,
                        contract_location
                    );

                    return;
                }

                let {
                    transaction,
                    contract_id,
                    target,
                    requester,
                    change_type,
                    timestamp,
                    contract_location,
                } = parse_put_msg_data(
                    contractChange,
                    contractChange.changeType()
                );

                if (change_type == ChangeType.PUT_REQUEST) {
                    handlePutRequest(
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type,
                        timestamp,
                        contract_location
                    );

                    return;
                }

                if (change_type == ChangeType.PUT_SUCCESS) {
                    handlePutSuccess(
                        transaction,
                        contract_id,
                        target,
                        requester,
                        change_type,
                        timestamp,
                        contract_location
                    );

                    return;
                }

                if (
                    contractChange.changeType() ===
                    fbTopology.ContractChangeType.PutFailure
                ) {
                    console.log("Put Failure");

                    // handlePutSuccess(
                    //     transaction,
                    //     contract_id,
                    //     target,
                    //     requester,
                    //     change_type
                    // );

                    return;
                }
            } catch (e) {
                console.error(e);
                errors.push(e);
            }

            try {
                const peerChange =
                    fbTopology.PeerChange.getRootAsPeerChange(buf);
                handleChange(peerChange);

                return;
            } catch (e) {
                errors.push(e);
            }

            if (errors.length > 0) {
                console.error("Failed to handle message:", errors);
            }
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
