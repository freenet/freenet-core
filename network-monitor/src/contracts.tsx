import { useEffect, useState } from "react";
import { TransactionData, TransactionPeerInterface, TxTableInterface } from "./type_definitions";
import {all_contracts} from "./transactions-data";
import {ContractDetail} from "./contract-detail";
import {filter_by_page, get_all_pages, rust_timestamp_to_utc_string} from "./utils";
import {Pagination} from "./pagination";

export const ContractsTable = ({ open_tx_detail, tx_list }: TxTableInterface) => {
    const [ordered_tx_list, set_ordered_tx_list] = useState<Array<TransactionData>>([]);
    const [inner_tx_list, set_inner_tx_list] = useState<Array<TransactionData>>([]);
    const [order_by, set_order_by] = useState<string>("timestamp");
    const [order_direction, set_order_direction] = useState<string>("asc");
    const [loading, set_loading] = useState<boolean>(true);
    const [page, set_page] = useState<number>(1);

    const order_tx_list = (tx_list: Array<TransactionData>) => {
        let updated_tx_list = tx_list;
        updated_tx_list = updated_tx_list.sort((a, b) => {
            if (order_by === "timestamp") {
                if (order_direction === "asc") {
                    return a.timestamp > b.timestamp ? 1 : -1;
                } else {
                    return a.timestamp < b.timestamp ? 1 : -1;
                }
            } else {
                return 0;
            }
        });

        return updated_tx_list;
    }



    useEffect(() => {
        if (tx_list) {
            let ordered_list = order_tx_list(tx_list);
            set_ordered_tx_list(ordered_list);
            set_inner_tx_list(order_tx_list(filter_by_page(tx_list, page)));


            if (loading) {
                setTimeout(() => {
                    set_loading(false);
                }, 1000);
            }
        }
    }, [tx_list, order_by, order_direction]);


    useEffect(() => {
        if (tx_list) {
            set_inner_tx_list(filter_by_page(ordered_tx_list, page));
        }
    }, [page]);


    return (
        <>
            <Pagination currentPage={page} totalPages={get_all_pages(tx_list || [])} 
                    onPageChange={(page: number) => {set_page(page)}}/>
            <table id="contracts" className="table is-striped block is-bordered">
                <thead id="contract-history-h">
                    <tr>
                        <th>Contract Key</th>
                        <th>Contract Location</th>
                        <th>Last Requester Peer Id</th>
                        <th>Last Target Peer Id</th>
                        <th>Last Transaction Id</th>
                        <th>Last Type</th>
                        <th>
                            Timestamp 
                            <button className={`button is-small is-outlined ml-1 ${loading ? "is-loading" : ""}`}
                                onClick={() => {
                                    set_loading(true);
                                    set_order_by("timestamp");
                                    set_order_direction(
                                        order_direction === "asc" ? "desc" : "asc"
                                    );
                                }}
                            >{order_direction}</button>
                        </th>
                        {/*<th>Status</th>
                        <th>Started</th>
                        <th>Finalized</th>*/}
                    </tr>
                </thead>
                <tbody id="contract-history-b">
                    {
                        inner_tx_list?.map((tx, index) => (
                            <tr key={`${tx.contract_id.slice(-8)}-${tx.change_type.slice(-8)}-${index}`}>
                                <td onClick={() => open_tx_detail(tx.contract_id)} style={{cursor: "pointer"}}>{tx.contract_id.slice(-8)}</td>
                                <td>{tx.contract_location}</td>
                                <td>{tx.requester.slice(-8)}</td>
                                <td>{tx.target.slice(-8)}</td>
                                <td>{tx.transaction_id.slice(-8)}</td>
                                <td>{tx.change_type}</td>
                                <td>{rust_timestamp_to_utc_string(tx.timestamp)}</td>
                                {/*<td>{tx.status}</td>
                                <td>{tx.started}</td>
                                <td>{tx.finalized}</td>*/}
                            </tr>
                        ))
                    }
                </tbody>
                
            </table>
        </>
    )
};



export function ContractsContainer() {
    const [is_detail_open, set_is_detail_open] = useState(false);
    const [transaction, set_transaction] = useState<TransactionData | null>(null);
    const [transaction_history, set_transaction_history] = useState<Array<TransactionData>>([]);
    const [peers_history, set_peers_history] = useState<Array<TransactionPeerInterface>>([]);
    const [tx_list, set_tx_list] = useState<Array<TransactionData>>([]);

    const open_tx_detail = (contract_id: string) => {
        console.log("Opening contract detail for contract id: ", contract_id);

        let contract_history = all_contracts.get(contract_id);
        if (!contract_history) {
            console.error("Transaction not found");
            return;
        }

        console.log("Contract history: ", contract_history);

        set_transaction(contract_history[0]);
        set_transaction_history(contract_history);
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

        set_tx_list(updated_tx_list);
    }

    document.addEventListener("keydown", (e: KeyboardEvent) => {
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

