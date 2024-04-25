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

    let transaction = put_request_obj.transaction()!;

    let contract_id = contractChange.contractId()!;

    let target = put_request_obj.target()!;

    let requester = put_request_obj.requester()!;

    let change_type = get_change_type(changeType)!;

    return {
        transaction,
        contract_id,
        target,
        requester,
        change_type,
    };
};
