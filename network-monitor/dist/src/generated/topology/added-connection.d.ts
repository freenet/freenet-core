import * as flatbuffers from "flatbuffers";
export declare class AddedConnection
  implements flatbuffers.IUnpackableObject<AddedConnectionT>
{
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): AddedConnection;
  static getRootAsAddedConnection(
    bb: flatbuffers.ByteBuffer,
    obj?: AddedConnection
  ): AddedConnection;
  static getSizePrefixedRootAsAddedConnection(
    bb: flatbuffers.ByteBuffer,
    obj?: AddedConnection
  ): AddedConnection;
  from(): string | null;
  from(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  fromLocation(): number;
  to(): string | null;
  to(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  toLocation(): number;
  static startAddedConnection(builder: flatbuffers.Builder): void;
  static addFrom(
    builder: flatbuffers.Builder,
    fromOffset: flatbuffers.Offset
  ): void;
  static addFromLocation(
    builder: flatbuffers.Builder,
    fromLocation: number
  ): void;
  static addTo(
    builder: flatbuffers.Builder,
    toOffset: flatbuffers.Offset
  ): void;
  static addToLocation(builder: flatbuffers.Builder, toLocation: number): void;
  static endAddedConnection(builder: flatbuffers.Builder): flatbuffers.Offset;
  static createAddedConnection(
    builder: flatbuffers.Builder,
    fromOffset: flatbuffers.Offset,
    fromLocation: number,
    toOffset: flatbuffers.Offset,
    toLocation: number
  ): flatbuffers.Offset;
  unpack(): AddedConnectionT;
  unpackTo(_o: AddedConnectionT): void;
}
export declare class AddedConnectionT implements flatbuffers.IGeneratedObject {
  from: string | Uint8Array | null;
  fromLocation: number;
  to: string | Uint8Array | null;
  toLocation: number;
  constructor(
    from?: string | Uint8Array | null,
    fromLocation?: number,
    to?: string | Uint8Array | null,
    toLocation?: number
  );
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
