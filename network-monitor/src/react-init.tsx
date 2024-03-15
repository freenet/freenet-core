import { ContractsTable } from "./contracts";
import { TransactionContainer } from "./transactions";
import { ContractsContainer } from "./contracts";
import {another_ring_visualization} from "./ring-visualization";

function ReactContainer() {
    return (
        <div>

        {another_ring_visualization({peerId: "abc", localization: 0.1}, [{peerId: "0x593b", localization: 0.3}, {peerId: "0x593b", localization: 0.5}, {peerId: "0x593b", localization: 0.7}, {peerId: "0x593b", localization: 0.9}])}
        <ContractsContainer />
        <TransactionContainer />
        
        </div>
        
    )
}

export const component = <ReactContainer />;


    // const localizations = [0.0001, 0.5498, 0.865, 0.988];

    // const points = localizations.map((x) => {
    //     return {
    //         x: Math.cos(2 * Math.PI * x),
    //         y: Math.sin(2 * Math.PI * x),
    //     };
    // }
//
//
//
                // <circle cx={60} cy={10} r="2.5" />
                // <circle cx={20} cy={50} r="2.5" />
                // <circle cx={100} cy={50} r="2.5" />
