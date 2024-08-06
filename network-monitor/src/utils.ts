import { ChangeType } from "./type_definitions";
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
): any => {
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
    };
};

export const rust_timestamp_to_utc_string = (timestamp: number): string => {
    return new Date(parseInt(timestamp.toString()) * 1000).toUTCString();
};
