import { ChangeType, PutMsgData, TransactionData } from "./type_definitions";
import { ContractChange } from "./generated/topology";
import * as fbTopology from "./generated/topology";

export const get_change_type = (
    change_type_fbs: fbTopology.ContractChangeType
): ChangeType | null => {
    switch (change_type_fbs) {
        case fbTopology.ContractChangeType.PutRequest:
            return ChangeType.PUT_REQUEST;
        case fbTopology.ContractChangeType.PutSuccess:
            return ChangeType.PUT_SUCCESS;
        case fbTopology.ContractChangeType.PutFailure:
            return ChangeType.PUT_FAILURE;
        default:
            new Error("Invalid change type");
    }

    return null;
};

export const parse_put_msg_data = (
    contractChange: ContractChange,
    changeType: fbTopology.ContractChangeType
): PutMsgData => {
    let put_request_obj = contractChange.change(new fbTopology.PutRequest());

    let transaction = put_request_obj.transaction();

    if (!transaction) {
        throw new Error("Transaction ID not found");
    }

    let contract_id = contractChange.contractId();

    if (!contract_id) {
        throw new Error("Contract ID not found");
    }

    let target = put_request_obj.target();

    if (!target) {
        throw new Error("Target Peer not found");
    }

    let requester = put_request_obj.requester();

    if (!requester) {
        throw new Error("Requester Peer not found");
    }

    let timestamp = put_request_obj.timestamp()!;

    let change_type = get_change_type(changeType)!;

    let contract_location = put_request_obj.contractLocation()!;

    return {
        transaction,
        contract_id,
        target,
        requester,
        change_type,
        timestamp,
        contract_location,
    } as PutMsgData;
};

export const rust_timestamp_to_utc_string = (timestamp: number): string => {
    return new Date(parseInt(timestamp.toString()) * 1000).toUTCString();
};

export const transactions_per_page = 10;

export const get_all_pages = (tx_list: Array<TransactionData>) => {
    return Math.ceil(tx_list.length / transactions_per_page);
};

export const filter_by_page = (
    tx_list: Array<TransactionData>,
    page: number
) => {
    let updated_tx_list = tx_list;
    let start = (page - 1) * transactions_per_page;
    let end = start + transactions_per_page;

    updated_tx_list = updated_tx_list.slice(start, end);

    return updated_tx_list;
};
