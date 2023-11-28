import * as fbTopology from "./generated/topology";
export declare let peers: PeerList;
interface PeerList {
  [id: string]: Peer;
}
interface Peer {
  id: string;
  currentLocation: number;
  connections: Connection[];
  history: ChangeInfo[];
  locationHistory: {
    location: number;
    timestamp: number;
  }[];
}
interface Connection {
  id: string;
  location: number;
}
interface ChangeInfo {
  type: "Added" | "Removed";
  from: Connection;
  to: Connection;
  timestamp: number;
}
export declare function handleChange(peerChange: fbTopology.PeerChange): void;
export declare function handleAddedConnection(
  peerChange: fbTopology.AddedConnectionT
): void;
export declare function handleRemovedConnection(
  peerChange: fbTopology.RemovedConnectionT
): void;
export declare function showConnections(peer: Peer): void;
export {};
