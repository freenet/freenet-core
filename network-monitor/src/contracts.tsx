import { useEffect, useState } from "react";
import { TransactionData, TransactionPeerInterface, TxTableInterface } from "./type_definitions";
import {all_contracts} from "./transactions-data";
import {ContractDetail} from "./contract-detail";

export const ContractsTable = ({ open_tx_detail, tx_list }: TxTableInterface) => (
    <table id="contracts" className="table is-striped block is-bordered">
        <thead id="contract-history-h">
            <tr>
                <th>Contract Key</th>
                <th>Last Requester Peer Id</th>
                <th>Last Target Peer Id</th>
                <th>Last Transaction Id</th>
                <th>Last Type</th>
                {/*<th>Status</th>
                <th>Started</th>
                <th>Finalized</th>*/}
            </tr>
        </thead>
        <tbody id="contract-history-b">
            {
                tx_list?.map((tx, index) => (
                    <tr key={`${tx.contract_id.slice(-8)}-${tx.change_type.slice(-8)}-${index}`}>
                        <td onClick={() => open_tx_detail(tx.contract_id)} style={{cursor: "pointer"}}>{tx.contract_id.slice(-8)}</td>
                        <td>{tx.requester.slice(-8)}</td>
                        <td>{tx.target.slice(-8)}</td>
                        <td>{tx.transaction_id.slice(-8)}</td>
                        <td>{tx.change_type}</td>
                        {/*<td>{tx.status}</td>
                        <td>{tx.started}</td>
                        <td>{tx.finalized}</td>*/}
                    </tr>
                ))
            }
        </tbody>
    </table>
);



export function ContractsContainer() {
    const [is_detail_open, set_is_detail_open] = useState(false);
    const [transaction, set_transaction] = useState<TransactionData | null>(null);
    const [transaction_history, set_transaction_history] = useState<Array<TransactionData>>([]);
    const [peers_history, set_peers_history] = useState<Array<TransactionPeerInterface>>([]);
    const [tx_list, set_tx_list] = useState<Array<TransactionData>>([]);

    const open_tx_detail = (contract_id: string) => {
        let contract_history = all_contracts.get(contract_id);
        if (!contract_history) {
            console.error("Transaction not found");
            return;
        }

        set_transaction(contract_history[0]);
        set_transaction_history(contract_history);
        // set_peers_history(mock_peers_in_tx[tx.id]);
        set_is_detail_open(true);
        window.scrollTo(0, 0);
    };

    const close_detail = () => {
        set_is_detail_open(false);
    };

    const load_contracts_list = () => {
        let updated_tx_list = [];
        
        updated_tx_list = Array.from(all_contracts.values());

        updated_tx_list = updated_tx_list.map((tx) => tx[tx.length - 1]);

        console.log(updated_tx_list);
        set_tx_list(updated_tx_list);
    }

    document.addEventListener("keydown", (e: any) => {
        if (e.key === "Escape") {
            close_detail();
        }
    });

    useEffect(() => {
        setInterval(() => {
            load_contracts_list();
        }, 3000);
    }, []);

    return (
        <div style={{marginBottom: 40}}>
            <ContractsTable open_tx_detail={open_tx_detail} tx_list={tx_list} />

            {transaction && transaction_history && (
                <ContractDetail
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

