use locutus_runtime::ContractInstanceId;

use crate::ring::PeerKeyLocation;

struct Meter {

}

enum AttributionSource {
    /// The resources were consumed 
    Peer(PeerKeyLocation),

    /// The resources were consumed by a contract we're relaying
    Contract(ContractInstanceId),
}