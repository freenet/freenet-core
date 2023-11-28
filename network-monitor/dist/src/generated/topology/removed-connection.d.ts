import * as flatbuffers from "flatbuffers";
export declare class RemovedConnection
  implements flatbuffers.IUnpackableObject<RemovedConnectionT>
{
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): RemovedConnection;
  static getRootAsRemovedConnection(
    bb: flatbuffers.ByteBuffer,
    obj?: RemovedConnection
  ): RemovedConnection;
  static getSizePrefixedRootAsRemovedConnection(
    bb: flatbuffers.ByteBuffer,
    obj?: RemovedConnection
  ): RemovedConnection;
  at(): string | null;
  at(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  from(): string | null;
  from(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  static startRemovedConnection(builder: flatbuffers.Builder): void;
  static addAt(
    builder: flatbuffers.Builder,
    atOffset: flatbuffers.Offset
  ): void;
  static addFrom(
    builder: flatbuffers.Builder,
    fromOffset: flatbuffers.Offset
  ): void;
  static endRemovedConnection(builder: flatbuffers.Builder): flatbuffers.Offset;
  static createRemovedConnection(
    builder: flatbuffers.Builder,
    atOffset: flatbuffers.Offset,
    fromOffset: flatbuffers.Offset
  ): flatbuffers.Offset;
  unpack(): RemovedConnectionT;
  unpackTo(_o: RemovedConnectionT): void;
}
export declare class RemovedConnectionT
  implements flatbuffers.IGeneratedObject
{
  at: string | Uint8Array | null;
  from: string | Uint8Array | null;
  constructor(
    at?: string | Uint8Array | null,
    from?: string | Uint8Array | null
  );
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
