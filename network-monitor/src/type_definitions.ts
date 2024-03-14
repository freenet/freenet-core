export type TransactionId = string;
export type ContractKey = string;

export enum ChangeType {
    PUT_REQUEST = "Put Request",
    PUT_SUCCESS = "Put Success",
    PUT_FAILURE = "Put Failure",
}

export type TransactionData = {
    change_type: ChangeType;
    transaction_id: string;
    contract_id: string;
    target: string;
    requester: string;
    status: string | null;
    started: string | null;
    finalized: string | null;
    unique_id: string;
};

export interface TransactionInterface {
    id: TransactionId;
    peer_id: string;
    type: ChangeType;
    contract_id: ContractKey;
    status: string | null;
    started: string | null;
    finalized: string | null;
}

export interface TransactionPeerInterface {
    peer_id: string;
    location: string;
    last_state: string;
    last_message: string;
    started: string;
    finalized: string;
}

export interface TransactionDetailInterface {
    transaction: TransactionData;
    is_displayed: boolean;
    close_detail: () => void;
    peers_history?: Array<TransactionPeerInterface>;
    tx_history?: Array<TransactionData>;
}

export interface TxTableInterface {
    open_tx_detail: (txid: TransactionId) => void;
    tx_list?: Array<TransactionData>;
}

export interface TxPeersTableInterface {
    [tx_id: TransactionId]: Array<TransactionPeerInterface>;
}

export enum TransactionType {
    Update = "Update",
    Put = "Put",
    Get = "Get",
}

export enum TransactionStatus {
    Received = "Received",
    Finalized = "Finalized",
    Ongoing = "Ongoing",
}

export enum MessageType {
    RequestUpdate = "RequestUpdate",
    SeekNode = "SeekNode",
    BroadcastTo = "BroadcastTo",
    Broadcasting = "Broadcasting",
    SuccessfulUpdate = "SuccessfulUpdate",
    UpdateForward = "UpdateForward",
}

export enum OpState {
    ReceivedRequest = "ReceivedRequest",
    AwaitingResponse = "AwaitingResponse",
    Finished = "Finished",
    PrepareRequest = "PrepareRequest",
    BroadcastOngoing = "BroadcastOngoing",
}
