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
    change_type: ChangeType
) {
    console.log("Put Request");
    console.log("tx", transaction_id);
    console.log("contract key", contract_id);
    console.log("target", target);
    console.log("requester", requester);

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        status: null,
        started: null,
        finalized: null,
    } as TransactionData;

    all_tx.set(transaction_id, [obj_data]);

    const this_contract_data = all_contracts.get(contract_id);
    if (!this_contract_data) {
        all_contracts.set(contract_id, [obj_data]);
    }
    all_contracts.get(contract_id)!.push(obj_data);
}

export function handlePutSuccess(
    transaction_id: string,
    contract_id: string,
    target: string,
    requester: string,
    change_type: ChangeType
) {
    console.log("Put Success");
    console.log("tx", transaction_id);
    console.log("contract key", contract_id);
    console.log("target", target);
    console.log("requester", requester);

    let obj_data = {
        change_type,
        transaction_id,
        contract_id,
        target,
        requester,
        status: null,
        started: null,
        finalized: null,
    };

    all_tx.set(transaction_id, [obj_data]);

    const this_contract_data = all_contracts.get(contract_id);
    if (!this_contract_data) {
        all_contracts.set(contract_id, [obj_data]);
    }
    all_contracts.get(contract_id)!.push(obj_data);
}
