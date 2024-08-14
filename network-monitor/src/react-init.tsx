import { ContractsTable } from "./contracts";
import { TransactionContainer } from "./transactions";
import { ContractsContainer } from "./contracts";
import {RingVisualization} from "./ring-visualization";
import {useEffect} from "react";
import {createRoot} from "react-dom/client";
import React from "react";



const ReactContainer = () => {


    useEffect(() => {
        // we need to wait here for the histogram to be initiated first. If we move all to React code we can remove this.
        setTimeout(() => {

            const ring_react_element = <RingVisualization main_peer={{peerId: "abc", localization: 0.1}}  other_peers={[{peerId: "0x593b", localization: 0.3}, {peerId: "0x593b", localization: 0.5}, {peerId: "0x593b", localization: 0.7}, {peerId: "0x593b", localization: 0.9}]}  />;


            // Append the SVG element.
            const peers_container = document.getElementById("peers-histogram")!;
            
            const ring_container = document.createElement("div");

            
            const root = createRoot(ring_container);
            root.render(ring_react_element);

            peers_container.appendChild(ring_container);
            
            console.log("React container mounted");

        }, 6000);


    }, []);


    return (
        <div>

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
