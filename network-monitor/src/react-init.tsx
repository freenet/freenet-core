import { ContractsTable } from "./contracts";
import { TransactionContainer } from "./transactions";
import { ContractsContainer } from "./contracts";

function ReactContainer() {
    return (
        <div><h1>Ale</h1>
        <ContractsContainer />
        <TransactionContainer />
        
        </div>
        
    )
}

export const component = <ReactContainer />;
