import { PeerId, RingVisualizationPoint } from "./type_definitions";
import {
    ChangeType,
    MessageType,
    OpState,
    TransactionInterface,
    TransactionStatus,
    TxPeersTableInterface,
} from "./type_definitions";

export const ring_mock_data: RingVisualizationPoint[] = [
    {
        peerId: "ideal",
        localization: 0.123485,
    },
    {
        peerId: "6f3360a9-ca27-4930-9fe7-578eedb5c9e7",
        localization: 0.802323,
    },
    {
        peerId: "38a93663-25e5-45ec-a708-7129d5eaf993",
        localization: 0.313894,
    },
    {
        peerId: "5210d7a5-5cec-4021-b4c9-dab6cbddd4f1",
        localization: 0.493189,
    },
    {
        peerId: "bf81df38-a14a-47ee-9673-aec6f49996a4",
        localization: 0.733043,
    },
    {
        peerId: "f79bbd6e-12f4-458f-9cab-8f61849943a3",
        localization: 0.900193,
    },
    {
        peerId: "aa97ae3d-858e-4f48-86ea-ae6dfbe13523",
        localization: 0.779211,
    },
];

const mock_transaction: Array<TransactionInterface> = [
    {
        id: "123",
        peer_id: "0xabc",
        type: ChangeType.PUT_SUCCESS,
        status: TransactionStatus.Finalized,
        started: "12-01-2024",
        finalized: "13-01-2024",
        contract_id: "0x123",
    },
    {
        id: "124",
        peer_id: "0xdef",
        type: ChangeType.PUT_SUCCESS,
        status: TransactionStatus.Received,
        started: "12-01-2024",
        finalized: "13-01-2024",
        contract_id: "0x4892",
    },
    {
        id: "125",
        peer_id: "0xabc",
        type: ChangeType.PUT_REQUEST,
        status: TransactionStatus.Ongoing,
        started: "12-02-2024",
        finalized: "13-02-2024",
        contract_id: "0x783",
    },
];

const mock_peers_in_tx: TxPeersTableInterface = {
    "123": [
        // create a mock object
        {
            peer_id: "0xabc",
            location: "0.1789234",
            last_state: OpState.BroadcastOngoing,
            last_message: MessageType.Broadcasting,
            started: "12-01-2024",
            finalized: "13-01-2024",
        },
        {
            peer_id: "0xdef",
            location: "0.16234",
            last_state: OpState.Finished,
            last_message: MessageType.SuccessfulUpdate,
            started: "18-01-2024",
            finalized: "19-01-2024",
        },
        {
            peer_id: "0xghi",
            location: "0.234234",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "19-01-2024",
            finalized: "20-01-2024",
        },
        {
            peer_id: "0xjkl",
            location: "0.267127",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "21-01-2024",
            finalized: "22-01-2024",
        },
        {
            peer_id: "0xdef",
            location: "0.1789234",
            last_state: OpState.BroadcastOngoing,
            last_message: MessageType.Broadcasting,
            started: "12-01-2024",
            finalized: "13-01-2024",
        },
        {
            peer_id: "0xabc",
            location: "0.16234",
            last_state: OpState.Finished,
            last_message: MessageType.SuccessfulUpdate,
            started: "18-01-2024",
            finalized: "19-01-2024",
        },
        {
            peer_id: "0xjkl",
            location: "0.234234",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "19-01-2024",
            finalized: "20-01-2024",
        },
        {
            peer_id: "0xghi",
            location: "0.267127",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "21-01-2024",
            finalized: "22-01-2024",
        },
    ],
    "124": [
        {
            peer_id: "0xdef",
            location: "0.1789234",
            last_state: OpState.BroadcastOngoing,
            last_message: MessageType.Broadcasting,
            started: "12-01-2024",
            finalized: "13-01-2024",
        },
        {
            peer_id: "0xabc",
            location: "0.16234",
            last_state: OpState.Finished,
            last_message: MessageType.SuccessfulUpdate,
            started: "18-01-2024",
            finalized: "19-01-2024",
        },
        {
            peer_id: "0xghi",
            location: "0.234234",
            last_state: OpState.ReceivedRequest,
            last_message: MessageType.BroadcastTo,
            started: "19-01-2024",
            finalized: "20-01-2024",
        },
        {
            peer_id: "0xjkl",
            location: "0.267127",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "21-01-2024",
            finalized: "22-01-2024",
        },
        {
            peer_id: "0xdef",
            location: "0.1789234",
            last_state: OpState.Finished,
            last_message: MessageType.SuccessfulUpdate,
            started: "12-01-2024",
            finalized: "13-01-2024",
        },
        {
            peer_id: "0xabc",
            location: "0.16234",
            last_state: OpState.Finished,
            last_message: MessageType.SuccessfulUpdate,
            started: "18-01-2024",
            finalized: "19-01-2024",
        },
        {
            peer_id: "0xjkl",
            location: "0.234234",
            last_state: OpState.AwaitingResponse,
            last_message: MessageType.RequestUpdate,
            started: "19-01-2024",
            finalized: "20-01-2024",
        },
        {
            peer_id: "0xghi",
            location: "0.267127",
            last_state: OpState.BroadcastOngoing,
            last_message: MessageType.Broadcasting,
            started: "21-01-2024",
            finalized: "22-01-2024",
        },
    ],
};

let another_ring_mock_data = [
    {
        id: new PeerId("1"),
        currentLocation: 0.123485,
        connectionTimestamp: 1234567890,
        connections: [],
        history: [],
        locationHistory: [],
    },
    {
        id: new PeerId("2"),
        currentLocation: 0.183485,
        connectionTimestamp: 1234567890,
        connections: [],
        history: [],
        locationHistory: [],
    },
    {
        id: new PeerId("3"),
        currentLocation: 0.323485,
        connectionTimestamp: 1234567890,
        connections: [],
        history: [],
        locationHistory: [],
    },
    {
        id: new PeerId("4"),
        currentLocation: 0.423485,
        connectionTimestamp: 1234567890,
        connections: [],
        history: [],
        locationHistory: [],
    },
    {
        id: new PeerId("5"),
        currentLocation: 0.783285,
        connectionTimestamp: 1234567890,
        connections: [],
        history: [],
        locationHistory: [],
    },
];
