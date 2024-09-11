import { ContractsTable } from "./contracts";
import { TransactionContainer } from "./transactions";
import { ContractsContainer } from "./contracts";
import {RingVisualization} from "./ring-visualization";
import {useEffect} from "react";
import {createRoot} from "react-dom/client";
import React from "react";
import {ring_mock_data} from "./mock_data";

declare global {
    interface Window {
        mermaid: any;
    }
}

const ReactContainer = () => {
    const [peers, setPeers] = React.useState(true);
    const [contracts, setContracts] = React.useState(false);
    
    const togglePeers = () => {
        setPeers(!peers);
    }

    const toggleContracts = () => {
        setContracts(!contracts);
    }

    useEffect(() => {
        console.log("Peers visibility changed");
        console.log(peers);
    }, [peers]);

    useEffect(() => {
        console.log("Contracts visibility changed");
        console.log(contracts);
    }, [contracts]);


    const ring_react_element = 
    <div>

        <RingVisualization main_peer={ring_mock_data[0]}  other_peers={ring_mock_data.slice(1)}  />

        <ButtonsElements togglePeers={togglePeers} toggleContracts={toggleContracts} />

    </div>;

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

        }, 5000);

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

interface IButtonElements {
    togglePeers: () => void;
    toggleContracts: () => void;
}

const ButtonsElements = ({togglePeers, toggleContracts}: IButtonElements) => (
                <div>
                    <button className="button" onClick={() => togglePeers()}>Peers</button>
                    <button className="button" onClick={() => toggleContracts()}>Contracts</button>
                </div>
);
