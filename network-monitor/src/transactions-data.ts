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
    let requester_location = parseFloat(
        requester.split(" (@ ")[1].split(")")[0]
    );

    requester = requester.split(" (@")[0];

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        requester_location,
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
    let requester_location = parseFloat(
        requester.split(" (@ ")[1].split(")")[0]
    );

    requester = requester.split(" (@")[0];

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        requester_location,
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
    let sender_location = parseFloat(sender.split(" (@")[1].split(")")[0]);
    sender = sender.split(" (@")[0];

    let upstream_location = upstream.split(" (@")[1].split(")")[0];
    upstream = upstream.split(" (@")[0];

    let obj_data = {
        change_type: ChangeType.BROADCAST_EMITTED,
        transaction_id,
        contract_id: key,
        target: broadcast_to,
        requester: sender,
        requester_location: sender_location,
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
    let requester_location = parseFloat(
        requester.split(" (@")[1].split(")")[0]
    );

    requester = requester.split(" (@")[0];

    let obj_data = {
        change_type,
        transaction_id,
        contract_id: key,
        target,
        requester,
        requester_location,
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

export function handleGetContract(
    transaction_id: string,
    requester: string,
    contract_id: string,
    contract_location: number,
    change_type: ChangeType,
    timestamp: number,
    target: string
) {
    let requester_location = parseFloat(
        requester.split(" (@ ")[1].split(")")[0]
    );
    requester = requester.split(" (@")[0];

    let target_location = parseFloat(target.split(" (@ ")[1].split(")")[0]);
    target = target.split(" (@")[0];

    let obj_data = {
        change_type,
        transaction_id,
        contract_id: contract_id,
        target,
        requester,
        unique_id:
            transaction_id +
            contract_id +
            contract_location +
            requester +
            change_type,
        timestamp,
        contract_location,
        requester_location,
    } as TransactionData;

    let tx_list = all_tx.get(transaction_id);

    if (tx_list) {
        all_tx.set(transaction_id, [...tx_list, obj_data]);
    } else {
        all_tx.set(transaction_id, [obj_data]);
    }

    const this_contract_data = all_contracts.get(contract_id);
    if (this_contract_data) {
        all_contracts.get(contract_id)!.push(obj_data);
    } else {
        all_contracts.set(contract_id, [obj_data]);
    }
}

export function handleSubscribedToContract(
    transaction_id: string,
    requester: string,
    contract_id: string,
    contract_location: number,
    change_type: ChangeType,
    at_peer: string,
    at_peer_location: number,
    timestamp: number
) {
    let requester_location = parseFloat(
        requester.split(" (@ ")[1].split(")")[0]
    );
    requester = requester.split(" (@")[0];

    let obj_data = {
        change_type,
        transaction_id,
        contract_id: contract_id,
        target: at_peer,
        requester,
        unique_id:
            transaction_id +
            contract_id +
            contract_location +
            requester +
            change_type,
        timestamp,
        contract_location,
        requester_location,
    } as TransactionData;

    let tx_list = all_tx.get(transaction_id);

    if (tx_list) {
        all_tx.set(transaction_id, [...tx_list, obj_data]);
    } else {
        all_tx.set(transaction_id, [obj_data]);
    }

    const this_contract_data = all_contracts.get(contract_id);
    if (this_contract_data) {
        all_contracts.get(contract_id)!.push(obj_data);
    } else {
        all_contracts.set(contract_id, [obj_data]);
    }
}
