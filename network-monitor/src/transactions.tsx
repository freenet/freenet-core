import { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import TransactionDetail from "./transaction-detail";
import { all_tx} from "./transactions-data";
import {TransactionInterface, TransactionStatus, TransactionType, TransactionData, TxPeersTableInterface, OpState, MessageType, TxTableInterface, TransactionPeerInterface, ChangeType } from "./type_definitions";
import {another_ring_visualization} from "./ring-visualization";
import {rust_timestamp_to_utc_string} from "./utils";






const TransactionsTable = ({ open_tx_detail, tx_list }: TxTableInterface) => (
    <table id="transactions" className="table is-striped block is-bordered">
        <thead id="transactions-history-h">
            <tr>
                <th>Transaction Id</th>
                <th>Requester Peer Id</th>
                <th>Target Peer Id</th>
                <th>Type</th>
                <th>Contract Key</th>
                <th>Timestamp</th>
                {/*<th>Status</th>
                <th>Started</th>
                <th>Finalized</th>*/}
            </tr>
        </thead>
        <tbody id="transactions-history-b">
            {
                tx_list?.map((tx, index) => (
                    <tr key={`${tx.transaction_id.slice(-8)}-${tx.change_type.slice(-8)}-${index}`}>
                        <td  onClick={() => open_tx_detail(tx.transaction_id)} style={{cursor: "pointer"}}>{tx.transaction_id.slice(-8)}</td>
                        <td>{tx.requester.slice(-8)}</td>
                        <td>{tx.target.slice(-8)}</td>
                        <td>{tx.change_type}</td>
                        <td>{tx.contract_id.slice(-8)}</td>
                        <td>{rust_timestamp_to_utc_string(tx.timestamp)}</td>
                        {/*<td>{tx.status}</td>
                        <td>{tx.started}</td>
                        <td>{tx.finalized}</td>*/}
                    </tr>
                ))
            }
        </tbody>
    </table>
);

export function TransactionContainer() {
    const [is_detail_open, set_is_detail_open] = useState(false);
    const [transaction, set_transaction] = useState<TransactionData | null>(null);
    const [transaction_history, set_transaction_history] = useState<Array<TransactionData>>([]);
    const [peers_history, set_peers_history] = useState<Array<TransactionPeerInterface>>([]);
    const [tx_list, set_tx_list] = useState<Array<TransactionData>>([]);

    const open_tx_detail = (txid: string) => {
        let tx_history = all_tx.get(txid);
        if (!tx_history) {
            console.error("Transaction not found");
            return;
        }

        set_transaction(tx_history[0]);
        set_transaction_history(tx_history);
        // set_peers_history(mock_peers_in_tx[tx.id]);
        set_is_detail_open(true);
        window.scrollTo(0, 0);
    };

    const close_detail = () => {
        set_is_detail_open(false);
    };

    const load_transaction_list = () => {
        let updated_tx_list = [];
        
        updated_tx_list = Array.from(all_tx.values());

        updated_tx_list = updated_tx_list.flat();

        console.log("updated_tx_list", updated_tx_list);
        set_tx_list(updated_tx_list);
    }

    document.addEventListener("keydown", (e: any) => {
        if (e.key === "Escape") {
            close_detail();
        }
    });

    useEffect(() => {
        setInterval(() => {
            load_transaction_list();
        }, 3000);
    }, []);

    return (
        <div>
            <TransactionsTable open_tx_detail={open_tx_detail} tx_list={tx_list} />

            {transaction && transaction_history && (
                <TransactionDetail
                    is_displayed={is_detail_open}
                    close_detail={close_detail}
                    transaction={transaction}
                    peers_history={peers_history}
                    tx_history={transaction_history}
                />
            )}
        </div>
    );
}

