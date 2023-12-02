import * as flatbuffers from "flatbuffers";
import {
  AddedConnection,
  AddedConnectionT,
} from "../topology/added-connection";
import { ErrorT } from "../topology/error";
import { PeerChangeType } from "../topology/peer-change-type";
import { RemovedConnectionT } from "../topology/removed-connection";
export declare class PeerChange
  implements flatbuffers.IUnpackableObject<PeerChangeT>
{
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): PeerChange;
  static getRootAsPeerChange(
    bb: flatbuffers.ByteBuffer,
    obj?: PeerChange
  ): PeerChange;
  static getSizePrefixedRootAsPeerChange(
    bb: flatbuffers.ByteBuffer,
    obj?: PeerChange
  ): PeerChange;
  currentState(index: number, obj?: AddedConnection): AddedConnection | null;
  currentStateLength(): number;
  changeType(): PeerChangeType;
  change<T extends flatbuffers.Table>(obj: any): any | null;
  static startPeerChange(builder: flatbuffers.Builder): void;
  static addCurrentState(
    builder: flatbuffers.Builder,
    currentStateOffset: flatbuffers.Offset
  ): void;
  static createCurrentStateVector(
    builder: flatbuffers.Builder,
    data: flatbuffers.Offset[]
  ): flatbuffers.Offset;
  static startCurrentStateVector(
    builder: flatbuffers.Builder,
    numElems: number
  ): void;
  static addChangeType(
    builder: flatbuffers.Builder,
    changeType: PeerChangeType
  ): void;
  static addChange(
    builder: flatbuffers.Builder,
    changeOffset: flatbuffers.Offset
  ): void;
  static endPeerChange(builder: flatbuffers.Builder): flatbuffers.Offset;
  static createPeerChange(
    builder: flatbuffers.Builder,
    currentStateOffset: flatbuffers.Offset,
    changeType: PeerChangeType,
    changeOffset: flatbuffers.Offset
  ): flatbuffers.Offset;
  unpack(): PeerChangeT;
  unpackTo(_o: PeerChangeT): void;
}
export declare class PeerChangeT implements flatbuffers.IGeneratedObject {
  currentState: AddedConnectionT[];
  changeType: PeerChangeType;
  change: AddedConnectionT | ErrorT | RemovedConnectionT | null;
  constructor(
    currentState?: AddedConnectionT[],
    changeType?: PeerChangeType,
    change?: AddedConnectionT | ErrorT | RemovedConnectionT | null
  );
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
