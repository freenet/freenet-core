import {
    ChangeType,
    ContractKey,
    TransactionData,
    TransactionId,
} from "./type_definitions";

export const all_tx = new Map<TransactionId, Array<TransactionData>>();

export const all_contracts = new Map<ContractKey, Array<TransactionData>>();

export function handlePutRequest(
    transaction_id: string,
    contract_id: string,
    target: string,
    requester: string,
    change_type: ChangeType,
    timestamp: number,
    contract_location: number
) {
    // console.log("Put Request");
    // console.log("tx", transaction_id);
    // console.log("contract key", contract_id);
    // console.log("target", target);
    // console.log("requester", requester);
    // console.log(
    //     "formatted timestamp",
    //     new Date(parseInt(timestamp.toString()) * 1000).toUTCString()
    // );
    // console.log("contract location", contract_location);

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        unique_id:
            transaction_id + contract_id + target + requester + change_type,
        timestamp,
        contract_location,
    } as TransactionData;

    if (
        all_tx
            .get(transaction_id)
            ?.find((obj) => obj.unique_id === obj_data.unique_id)
    ) {
        return;
    }

    all_tx.set(transaction_id, [obj_data]);

    const this_contract_data = all_contracts.get(contract_id);
    if (!this_contract_data) {
        all_contracts.set(contract_id, []);
    }
    all_contracts.get(contract_id)!.push(obj_data);
}

export function handlePutSuccess(
    transaction_id: string,
    contract_id: string,
    target: string,
    requester: string,
    change_type: ChangeType,
    timestamp: number,
    contract_location: number
) {
    // console.log("Put Success");
    // console.log("tx", transaction_id);
    // console.log("contract key", contract_id);
    // console.log("change_type", change_type);
    // console.log("target", target);
    // console.log("requester", requester);
    // console.log("timestamp", timestamp);
    // console.log("contract location", contract_location);

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        status: undefined,
        started: undefined,
        finalized: undefined,
        unique_id:
            transaction_id + contract_id + target + requester + change_type,
        timestamp,
        contract_location,
    } as TransactionData;

    if (
        all_tx
            .get(transaction_id)
            ?.find((obj) => obj.unique_id === obj_data.unique_id)
    ) {
        return;
    }

    all_tx.get(transaction_id)!.push(obj_data);

    const this_contract_data = all_contracts.get(contract_id);
    if (!this_contract_data) {
        all_contracts.set(contract_id, []);
    }
    all_contracts.get(contract_id)!.push(obj_data);
}

export function handleBroadcastEmitted(
    transaction_id: string,
    upstream: string,
    broadcast_to: any[],
    key: string,
    sender: string,
    timestamp: number,
    contract_location: number
) {
    let sender_location = sender.split(" (@")[1].split(")")[0];
    sender = sender.split(" (@")[0];

    let upstream_location = upstream.split(" (@")[1].split(")")[0];
    upstream = upstream.split(" (@")[0];

    // console.log("Broadcast Emitted");
    // console.log("tx", transaction_id);
    // console.log("upstream", upstream);
    // console.log("broadcast to", broadcast_to);
    // console.log("key", key);
    // console.log("sender", sender);
    // console.log("timestamp", timestamp);
    // console.log("contract location", contract_location);

    let obj_data = {
        change_type: ChangeType.BROADCAST_EMITTED,
        transaction_id,
        contract_id: key,
        target: broadcast_to,
        requester: sender,
        unique_id: transaction_id + key + broadcast_to[0] + sender,
        timestamp,
        contract_location,
        upstream,
    } as TransactionData;

    if (
        all_tx
            .get(transaction_id)
            ?.find((obj) => obj.unique_id === obj_data.unique_id)
    ) {
        return;
    }

    all_tx.get(transaction_id)!.push(obj_data);

    const this_contract_data = all_contracts.get(key);
    if (!this_contract_data) {
        console.error("Contract not found when logging Broadcast Emitted");
    }
    all_contracts.get(key)!.push(obj_data);
}

export function handleBroadcastReceived(
    transaction_id: string,
    target: string,
    requester: string,
    key: string,
    change_type: ChangeType,
    timestamp: number,
    contract_location: number
) {
    // console.log("Broadcast Received");
    // console.log("tx", transaction_id);
    // console.log("target", target);
    // console.log("requester", requester);
    // console.log("key", key);
    // console.log("change_type", change_type);
    // console.log("timestamp", timestamp);
    // console.log("contract location", contract_location);

    let obj_data = {
        change_type,
        transaction_id,
        contract_id: key,
        target,
        requester,
        unique_id: transaction_id + key + target + requester,
        timestamp,
        contract_location,
    } as TransactionData;

    if (
        all_tx
            .get(transaction_id)
            ?.find((obj) => obj.unique_id === obj_data.unique_id)
    ) {
        return;
    }

    all_tx.get(transaction_id)!.push(obj_data);

    const this_contract_data = all_contracts.get(key);
    if (!this_contract_data) {
        console.error("Contract not found when logging Broadcast Received");
    }

    all_contracts.get(key)!.push(obj_data);
}
