import { AddedConnection } from "../topology/added-connection";
import { Error } from "../topology/error";
import { RemovedConnection } from "../topology/removed-connection";
export declare enum PeerChangeType {
  NONE = 0,
  AddedConnection = 1,
  RemovedConnection = 2,
  Error = 3,
}
export declare function unionToPeerChangeType(
  type: PeerChangeType,
  accessor: (
    obj: AddedConnection | Error | RemovedConnection
  ) => AddedConnection | Error | RemovedConnection | null
): AddedConnection | Error | RemovedConnection | null;
export declare function unionListToPeerChangeType(
  type: PeerChangeType,
  accessor: (
    index: number,
    obj: AddedConnection | Error | RemovedConnection
  ) => AddedConnection | Error | RemovedConnection | null,
  index: number
): AddedConnection | Error | RemovedConnection | null;
