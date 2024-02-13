import { useState } from "react";
import { createRoot } from "react-dom/client";
import TransactionDetail from "./transaction-detail";


export interface TransactionInterface {
    id: string;
    peer_id: string;
    type: string;
    status: string;
    started: string;
    finalized: string;
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
    transaction: TransactionInterface;
    is_displayed: boolean;
    close_detail: () => void;
    peers_history: Array<TransactionPeerInterface>;
}

interface TxTableInterface {
    open_tx_detail: (txid: string) => void;
    tx_list?: Array<TransactionInterface>;
}

interface TxPeersTableInterface {
    [tx_id: string]: Array<TransactionPeerInterface>
}

enum TransactionType {
    Update = "Update",
    Put = "Put",
    Get = "Get",
}

enum TransactionStatus {
    Received = "Received",
    Finalized = "Finalized",
    Ongoing = "Ongoing",
}

enum MessageType {
    RequestUpdate = "RequestUpdate",
    SeekNode = "SeekNode",
    BroadcastTo = "BroadcastTo",
    Broadcasting = "Broadcasting",
    SuccessfulUpdate = "SuccessfulUpdate",
    UpdateForward = "UpdateForward",
}

enum OpState {
    ReceivedRequest = "ReceivedRequest",
    AwaitingResponse = "AwaitingResponse",
    Finished = "Finished",
    PrepareRequest = "PrepareRequest",
    BroadcastOngoing = "BroadcastOngoing",
}

const mock_transaction: Array<TransactionInterface> = [
    {
        id: "123",
        peer_id: "0xabc",
        type: TransactionType.Update,
        status: TransactionStatus.Finalized,
        started: "12-01-2024",
        finalized: "13-01-2024",
    },
    {
        id: "124",
        peer_id: "0xdef",
        type: TransactionType.Put,
        status: TransactionStatus.Received,
        started: "12-01-2024",
        finalized: "13-01-2024",
    },
    {
        id: "125",
        peer_id: "0xabc",
        type: TransactionType.Get,
        status: TransactionStatus.Ongoing,
        started: "12-02-2024",
        finalized: "13-02-2024",
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
}



const TransactionsTable = ({ open_tx_detail, tx_list }: TxTableInterface) => (
    <table id="transactions" className="table is-striped block is-bordered">
        <thead id="transactions-history-h">
            <tr>
                <th>Transaction Id</th>
                <th>Peer Id</th>
                <th>Type</th>
                <th>Status</th>
                <th>Started</th>
                <th>Finalized</th>
            </tr>
        </thead>
        <tbody id="transactions-history-b">
            {
                tx_list?.map((tx) => (
                    <tr>
                        <td onClick={() => open_tx_detail(tx.id)} style={{cursor: "pointer"}}>{tx.id}</td>
                        <td>{tx.peer_id}</td>
                        <td>{tx.type}</td>
                        <td>{tx.status}</td>
                        <td>{tx.started}</td>
                        <td>{tx.finalized}</td>
                    </tr>
                ))
            }
        </tbody>
    </table>
);

function TransactionContainer() {
    const [is_detail_open, set_is_detail_open] = useState(false);
    const [transaction, set_transaction] = useState<TransactionInterface | null>(null);
    const [peers_history, set_peers_history] = useState<Array<TransactionPeerInterface>>([]);

    const open_tx_detail = (txid: string) => {
        let tx = mock_transaction.find((tx) => tx.id === txid);
        if (!tx) {
            console.error("Transaction not found");
            return;
        }
        set_transaction(tx);
        set_peers_history(mock_peers_in_tx[tx.id]);
        set_is_detail_open(true);
    };

    const close_detail = () => {
        set_is_detail_open(false);
    };

    document.addEventListener("keydown", (e: any) => {
        if (e.key === "Escape") {
            close_detail();
        }
    });

    return (
        <div>
            <h1>Hello, world</h1>
            <TransactionsTable open_tx_detail={open_tx_detail} tx_list={mock_transaction} />

            {transaction && (
                <TransactionDetail
                    is_displayed={is_detail_open}
                    close_detail={close_detail}
                    transaction={transaction}
                    peers_history={peers_history}
                />
            )}
        </div>
    );
}

export const component = <TransactionContainer />;
