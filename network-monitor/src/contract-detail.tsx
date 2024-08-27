import { useEffect, useState } from "react";
import {
    TransactionDetailInterface,
    TransactionData,
    TransactionDetailPeersHistoryInterface,
    FilterDictionaryInterface,
    ContractHistoryInterface
} from "./type_definitions";
import {filter_by_page, get_all_pages, rust_timestamp_to_utc_string} from "./utils";
import {Pagination} from "./pagination";


const ContractPeersHistory = ({
    tx_peer_list,
}: TransactionDetailPeersHistoryInterface) => {
    const [filter, set_filter] = useState<FilterDictionaryInterface>({});
    const [filtered_list, set_filtered_list] = useState(tx_peer_list);
    const [order_by, set_order_by] = useState<string>("timestamp");
    const [order_direction, set_order_direction] = useState<string>("asc");
    const [order_is_loading, set_order_is_loading] = useState<boolean>(false);
    const [page, set_page] = useState<number>(1);
    const [inner_tx_list, set_inner_tx_list] = useState<Array<TransactionData>>([]);

    const add_filter = (filter_type: string, filter_value: string) => {
        if (check_if_contains_filter(filter_type)) {
            return;
        }

        filter[filter_type] = {
            filter_type,
            filter_value,
        };

        set_filter(filter);

        update_filtered_list();
    };

    const update_filtered_list = () => {
        let filtered_list = tx_peer_list;

        Object.keys(filter).forEach((filter_type) => {
            const filter_value = filter[filter_type].filter_value;
            filtered_list = filtered_list.filter((tx) => {
                for (const [key, value] of Object.entries(tx)) {
                    if (key === filter_type) {
                        return value === filter_value;
                    }
                }
            });
        });

        console.log("filtered_list", filtered_list);

        filtered_list = filtered_list.sort((a, b) => {
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

        set_inner_tx_list(filter_by_page(filtered_list, page));
        set_filtered_list(filtered_list);
    };

    const clear_one_filter = (filter_type: string) => {
        delete filter[filter_type];
        update_filtered_list();
    };

    const clear_all_filters = () => {
        set_filter({});
    };

    useEffect(() => {
        update_filtered_list();
        
        if (order_is_loading) {
            setTimeout(() => {
                    set_order_is_loading(false);
            }, 500)
        }
    }, [filter, order_by, order_direction]);

    useEffect(() => {
        update_filtered_list();
    }, [tx_peer_list]);

    useEffect(() => {
        set_inner_tx_list(filter_by_page(filtered_list, page));
    }, [page, filtered_list]);   


    const check_if_contains_filter = (filter_type: string) => {
        return filter[filter_type] !== undefined;
    };

    return (
        <div
            id="transaction-peers-history"
            className="block"
            style={{ marginTop: 20 }}
        >
            <Pagination currentPage={page} totalPages={get_all_pages(filtered_list)} onPageChange={(page:number) => set_page(page)}/>
            <h2>Contract Transactions History </h2>
            {Object.keys(filter).length > 0 && (
                <div>
                    <button className="button is-info mb-2" onClick={() => clear_all_filters()}>
                        Clear all filters
                    </button>
                </div>
            )}
            <table
                id="transaction-peers-history"
                className="table is-striped block is-bordered"
            >
                <thead id="transaction-peers-history-h">
                    <tr>
                        <th>
                            Contract Id
                            {check_if_contains_filter("contract_id") && (
                                <button className="button is-small is-outlined ml-1 pr-1 pl-1" 
                                    onClick={() =>
                                        clear_one_filter("contract_id")
                                    }
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Contract Location
                        </th>
                        <th>
                            Transaction Id
                            {check_if_contains_filter("transaction_id") && (
                                <button className="button is-small is-outlined ml-1 pr-1 pl-1" 
                                    onClick={() => clear_one_filter("transaction_id")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Requester
                            {check_if_contains_filter("requester") && (
                                <button className="button is-small is-outlined ml-1 pr-1 pl-1" 
                                    onClick={() => clear_one_filter("requester")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Target
                            {check_if_contains_filter("target") && (
                                <button className="button is-small is-outlined ml-1 pr-1 pl-1" 
                                    onClick={() => clear_one_filter("target")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Change Type
                            {check_if_contains_filter("change_type") && (
                                <button className="button is-small is-outlined ml-1 pr-1 pl-1" 
                                    onClick={() =>
                                        clear_one_filter("change_type")
                                    }
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Timestamp
                            <button className={`button is-small is-outlined ml-1 ${order_is_loading ? "is-loading" : ""}`}
                                onClick={() => {
                                    set_order_is_loading(true);
                                    set_order_by("timestamp");
                                    set_order_direction(
                                        order_direction === "asc" ? "desc" : "asc"
                                    );
                                }}
                            >{order_direction}</button>

                        </th>
                    </tr>
                </thead>
                <tbody id="transaction-peers-history-b">
                    {inner_tx_list.map((tx) => (
                        <tr>
                            <td
                                onClick={() =>
                                    add_filter("contract_id", tx.contract_id)
                                }
                                style={{
                                    cursor: "pointer",
                                }}
                            >
                                {tx.contract_id.slice(-8)}
                            </td>
                            <td>
                                {tx.contract_location}
                            </td>
                            <td
                                onClick={() =>
                                    add_filter("transaction_id", tx.transaction_id)
                                }
                                style={{
                                    cursor: "pointer",
                                }}
                            >
                                {tx.transaction_id}
                            </td>
                            <td
                                onClick={() =>
                                    add_filter("requester", tx.requester)
                                }
                                style={{
                                    cursor: "pointer",
                                }}
                            >
                                {tx.requester.slice(-8)}
                            </td>
                            <td
                                onClick={() =>
                                    add_filter("target", tx.target)
                                }
                                style={{
                                    cursor: "pointer",
                                }}
                            >
                                {tx.target.slice(-8)}
                            </td>
                            <td
                                onClick={() =>
                                    add_filter("change_type", tx.change_type)
                                }
                                style={{
                                    cursor: "pointer",
                                }}
                            >
                                {tx.change_type}
                            </td>
                            <td>
                                {rust_timestamp_to_utc_string(tx.timestamp)}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};



// TODO: use real types
const ContractHistory = ({ contract_history }: ContractHistoryInterface) => (
    <div id="contract-history" className="block">
        <h2>Contract History</h2>
        <table
            id="transaction-history"
            className="table is-striped block is-bordered"
        >
            <thead id="contract-history-h">
                <tr>
                    <th>Contract Key</th>
                    <th>Transaction Id</th>
                    <th>Requester Peer Id</th>
                    <th>Target Peer Id</th>
                    <th>Type</th>
                    {/*<th>Status</th>
                    <th>Started</th>
                    <th>Finalized</th>*/}
                </tr>
            </thead>
            <tbody id="contract-history-b">
                {contract_history &&
                    contract_history.map((change: TransactionData, index: number) => (
                        <tr key={`${change.transaction_id.slice(-8)}-${change.change_type.slice(-8)}-${index}`}>
                            <td>{change.contract_id.slice(-8)}</td>
                            <td>{change.transaction_id.slice(-8)}</td>
                            <td>{change.requester.slice(-8)}</td>
                            <td>{change.target.slice(-8)}</td>
                            <td>{change.change_type}</td>
                        </tr>
                    ))}
            </tbody>
        </table>
    </div>
);

export const ContractDetail = ({
    transaction,
    is_displayed,
    close_detail,
    peers_history,
    tx_history,
}: TransactionDetailInterface) => (
    <div
        id="contract-detail"
        style={{
            position: "absolute",
            top: 0,
            left: 0,
            width: "95%",
            height: "95%",
            display: is_displayed ? "flex" : "none",
            justifyContent: "center",
            alignItems: "center",
        }}
    >
        <div
            style={{
                border: "2px solid black",
                padding: 30,
                marginTop: 20,
                width: "75%",
                height: "75%",
                backgroundColor: "rgba(255, 255, 255, 0.95)",
                position: "relative",
                overflow: "scroll",
            }}
        >
            <button
                id="transaction-detail-close"
                style={{
                    position: "absolute",
                    top: 0,
                    right: 0,
                    margin: 10,
                }}
                onClick={close_detail}
            >
                X
            </button>
            <h2>Contract Details</h2>
            <div id="transaction-detail-contents">
                <p>Contract Key {transaction.contract_id}</p>
                <p>Contract Location {transaction.contract_location}</p>
                <p>Requester {transaction.requester}</p>
                <p>Target {transaction.target}</p>
                {/*<p>Status {transaction.status}</p>
                <p>Started {transaction.started}</p>
                <p>Finalized {transaction.finalized}</p>*/}
            </div>

            <div>
                <h2>Location Peers</h2>
                <div id="peers-histogram"></div>
            </div>

            <div id="other-peer-conns-graph">
            </div>

            {tx_history && (
                <ContractPeersHistory tx_peer_list={tx_history} />
            )}

            {/*<ContractHistory contract_history={tx_history} />*/}
        </div>
    </div>
);


