import { ContractsTable } from "./contracts";
import { TransactionContainer } from "./transactions";
import { ContractsContainer } from "./contracts";
import {RingVisualization} from "./ring-visualization";
import {useEffect} from "react";
import {createRoot} from "react-dom/client";
import React from "react";
import {ring_mock_data} from "./mock_data";
import {HomePageRing} from "./home-page-ring";

declare global {
    interface Window {
        mermaid: any;
    }
}

const ReactContainer = () => {
    const ring_react_element = <HomePageRing />;

    useEffect(() => {
        // we need to wait here for the histogram to be initiated first. If we move all to React code we can remove this.
        setTimeout(() => {
            // Append the SVG element.
            const peers_container = document.getElementById("peers-histogram")!;
            
            const ring_container = document.createElement("div");
            
            const root = createRoot(ring_container);
            root.render(ring_react_element);

            peers_container.appendChild(ring_container);


            console.log("React container mounted");

        }, 4000);

        window.mermaid.contentLoaded();
    }, []);


    return (
        <div>

            <ContractsContainer />
            <TransactionContainer />

        
        </div>
        
    )
}

export const component = <ReactContainer />;

