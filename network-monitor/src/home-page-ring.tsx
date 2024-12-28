import React, {useEffect} from "react";
import {RingVisualization} from "./ring-visualization";
import {ring_mock_data} from "./mock_data";
import {all_contracts} from "./transactions-data";
import {ChangeType, RingVisualizationPoint} from "./type_definitions";


export const HomePageRing = () => {
    let [peers, setPeers] = React.useState(true);
    let [contracts, setContracts] = React.useState(false);

    let [contractsData, setContractsData] = React.useState<RingVisualizationPoint[]>([]);

    let togglePeers = () => {
        setPeers(!peers);
    }

    let toggleContracts = () => {
        setContracts(!contracts);
    }

    useEffect(() => {
        console.log("Peers visibility changed");
        console.log(peers);
    }, [peers]);

    useEffect(() => {
        console.log("Contracts visibility changed");
        console.log(contracts);

        updateContracts();

    }, [contracts]);


    const updateContracts = () => {
        let all_contracts_locations = new Array<RingVisualizationPoint>();

        
        for (let key of all_contracts.keys()) {
            let one_contract = all_contracts.get(key)!;
            let put_success = one_contract.find(e => e.change_type == ChangeType.PUT_SUCCESS);
            if (put_success) {
                all_contracts_locations.push({
                    localization: put_success.requester_location as number,
                    peerId: put_success.requester as string,
                } as RingVisualizationPoint);
            }                                               
        }

        console.log("all_contracts_locations", all_contracts_locations);

        let key_index = all_contracts.keys().next().value as string;
        let one_contract = all_contracts.get(key_index);

        console.log("key_index", key_index);
        console.log("one_contract", one_contract);

        setContractsData(all_contracts_locations);

    }


    return (
        <div>

            <RingVisualization other_peers={contractsData} selected_text={"Contract"} />

            <ButtonsElements togglePeers={togglePeers} toggleContracts={toggleContracts} />

        </div>
    );
}




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
