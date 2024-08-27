export type TransactionId = string;
export type ContractKey = string;

export enum ChangeType {
    PUT_REQUEST = "Put Request",
    PUT_SUCCESS = "Put Success",
    PUT_FAILURE = "Put Failure",
    BROADCAST_EMITTED = "Broadcast Emitted",
    BROADCAST_RECEIVED = "Broadcast Received",
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
    timestamp: number;
    contract_location: number;
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

export type PutMsgData = {
    transaction: string;
    contract_id: string;
    target: string;
    requester: string;
    change_type: ChangeType;
    timestamp: number;
    contract_location: number;
};

export interface TransactionDetailPeersHistoryInterface {
    tx_peer_list: Array<TransactionData>;
}

export interface FilterInterface {
    filter_type: string;
    filter_value: string;
}

export interface FilterDictionaryInterface {
    [key: string]: FilterInterface;
}

export interface ContractHistoryInterface {
    contract_history: Array<TransactionData>;
}

export interface RingVisualizationPoint {
    peerId: string;
    localization: number;
}

export interface RingVisualizationProps {
    main_peer: RingVisualizationPoint;
    other_peers: RingVisualizationPoint[];
}

export interface PeerList {
    [id: string]: Peer;
}

export interface Peer {
    id: PeerId;
    currentLocation: number;
    connectionTimestamp: number;
    connections: Connection[];
    history: ChangeInfo[];
    locationHistory: { location: number; timestamp: number }[];
}

export interface Connection {
    transaction: string | null;
    id: PeerId;
    location: number;
}

export interface ChangeInfo {
    type: "Added" | "Removed";
    from: Connection;
    to: Connection;
    timestamp: number;
}

export class PeerId {
    private id: string;

    constructor(id: string | Uint8Array) {
        if (id instanceof Uint8Array) {
            this.id = new TextDecoder().decode(id);
        } else {
            this.id = id;
        }
    }

    get short() {
        return this.id.slice(-8);
    }

    get full() {
        return this.id;
    }
}

export interface TransactionDetailPeersHistoryInterface {
    tx_peer_list: Array<TransactionData>;
}

export interface FilterInterface {
    filter_type: string;
    filter_value: string;
}

export interface TranscationHistoryInterface {
    tx_history: Array<TransactionData>;
}
