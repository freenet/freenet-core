import { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
    TransactionDetailInterface,
    TransactionPeerInterface,
    TransactionData
} from "./type_definitions";
import { PeerId } from "./topology";

interface TransactionDetailPeersHistoryInterface {
    tx_peer_list: Array<TransactionData>;
}

interface FilterInterface {
    filter_type: string;
    filter_value: string;
}

interface FilterDictionaryInterface {
    [key: string]: FilterInterface;
}

let ring_mock_data = [];
const TransactionPeersHistory = ({
    tx_peer_list,
}: TransactionDetailPeersHistoryInterface) => {
    const [filter, set_filter] = useState<FilterDictionaryInterface>({});
    const [filtered_list, set_filtered_list] = useState(tx_peer_list);

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

        let ring_mock_data = [
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

        // ringHistogram(ring_mock_data);

        // const graphContainer = d3.select(
        //     document.getElementById("other-peer-conns-graph")
        // );

        // ringVisualization(ring_mock_data[0], graphContainer, 1.25);
    }, [filter]);

    const check_if_contains_filter = (filter_type: string) => {
        return filter[filter_type] !== undefined;
    };

    return (
        <div
            id="transaction-peers-history"
            className="block"
            style={{ marginTop: 20 }}
        >
            <h2>Transaction Peers History </h2>
            {Object.keys(filter).length > 0 && (
                <div>
                    <button onClick={() => clear_all_filters()}>
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
                            Transaction Id
                            {check_if_contains_filter("transaction_id") && (
                                <button
                                    onClick={() => clear_one_filter("transaction_id")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Requester
                            {check_if_contains_filter("requester") && (
                                <button
                                    onClick={() => clear_one_filter("requester")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Target
                            {check_if_contains_filter("target") && (
                                <button
                                    onClick={() => clear_one_filter("target")}
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Change Type
                            {check_if_contains_filter("change_type") && (
                                <button
                                    onClick={() =>
                                        clear_one_filter("change_type")
                                    }
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                        <th>
                            Contract Id
                            {check_if_contains_filter("contract_id") && (
                                <button
                                    onClick={() =>
                                        clear_one_filter("contract_id")
                                    }
                                >
                                    Clear filter
                                </button>
                            )}
                        </th>
                    </tr>
                </thead>
                <tbody id="transaction-peers-history-b">
                    {filtered_list.map((tx) => (
                        <tr>
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
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

// TODO: use real types
const TransactionHistory = ({ tx_history }: any) => (
    <div id="transaction-history" className="block">
        <h2>Transaction History</h2>
        <table
            id="transaction-history"
            className="table is-striped block is-bordered"
        >
            <thead id="transaction-history-h">
                <tr>
                    <th>Transaction Id</th>
                    <th>Requester Peer Id</th>
                    <th>Target Peer Id</th>
                    <th>Type</th>
                    <th>Contract Key</th>
                    {/*<th>Status</th>
                    <th>Started</th>
                    <th>Finalized</th>*/}
                </tr>
            </thead>
            <tbody id="transaction-history-b">
                {tx_history &&
                    tx_history.map((change: TransactionData, index: number) => (
                        <tr key={`${change.transaction_id.slice(-8)}-${change.change_type.slice(-8)}-${index}`}>
                            <td>{change.transaction_id.slice(-8)}</td>
                            <td>{change.requester.slice(-8)}</td>
                            <td>{change.target.slice(-8)}</td>
                            <td>{change.change_type}</td>
                            <td>{change.contract_id.slice(-8)}</td>
                        </tr>
                    ))}
            </tbody>
        </table>
    </div>
);

const TransactionDetail = ({
    transaction,
    is_displayed,
    close_detail,
    peers_history,
    tx_history,
}: TransactionDetailInterface) => (
    <div
        id="transaction-detail"
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
            <h2>Transaction Detail</h2>
            <div id="transaction-detail-contents">
                <p>ID {transaction.transaction_id}</p>
                <p>Type {transaction.change_type}</p>
                {/*<p>Status {transaction.status}</p>
                <p>Started {transaction.started}</p>
                <p>Finalized {transaction.finalized}</p>*/}
                <p>Requester {transaction.requester}</p>
                <p>Target {transaction.target}</p>
                <p>Contract Key {transaction.contract_id}</p>
            </div>

            <div>
                <h2>Location Peers</h2>
                <div id="peers-histogram"></div>
            </div>

            <div id="other-peer-conns-graph">
            {/*another_ring_visualization()*/}
            </div>

            {tx_history && (
                <TransactionPeersHistory tx_peer_list={tx_history} />
            )}

            {/*<TransactionHistory tx_history={tx_history} />*/}
        </div>
    </div>
);

const another_ring_visualization = () => {
    // Declare the chart dimensions and margins.
    const width = 640;
    const height = 400;
    const marginTop = 20;
    const marginRight = 20;
    const marginBottom = 30;
    const marginLeft = 40;

    // // Declare the x (horizontal position) scale.
    // const x = d3
    //     .scaleUtc()
    //     .domain([new Date("2023-01-01"), new Date("2024-01-01")])
    //     .range([marginLeft, width - marginRight]);

    // // Declare the y (vertical position) scale.
    // const y = d3
    //     .scaleLinear()
    //     .domain([0, 100])
    //     .range([height - marginBottom, marginTop]);

    // // Create the SVG container.
    // const svg = d3.create("svg").attr("width", width).attr("height", height);

    // // Add the x-axis.
    // svg.append("g")
    //     .attr("transform", `translate(0,${height - marginBottom})`)
    //     .call(d3.axisBottom(x));

    // // Add the y-axis.
    // svg.append("g")
    //     .attr("transform", `translate(${marginLeft},0)`)
    //     .call(d3.axisLeft(y));

    let a = (
        <svg width={300} height={100}>
            <path
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                d={"20"}
            />
            <g fill="white" stroke="currentColor" strokeWidth="1.5">
                <circle key={"123"} cx={60} cy={50} r="40.5" />

                <circle cx={60} cy={10} r="2.5" />
                <circle cx={10} cy={50} r="2.5" />
                <circle cx={110} cy={50} r="2.5" />
            </g>
        </svg>
    );

    // Append the SVG element.
    return a;
};

export default TransactionDetail;
