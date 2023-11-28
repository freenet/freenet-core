import * as flatbuffers from "flatbuffers";
export declare class Ok implements flatbuffers.IUnpackableObject<OkT> {
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): Ok;
  static getRootAsOk(bb: flatbuffers.ByteBuffer, obj?: Ok): Ok;
  static getSizePrefixedRootAsOk(bb: flatbuffers.ByteBuffer, obj?: Ok): Ok;
  message(): string | null;
  message(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  static startOk(builder: flatbuffers.Builder): void;
  static addMessage(
    builder: flatbuffers.Builder,
    messageOffset: flatbuffers.Offset
  ): void;
  static endOk(builder: flatbuffers.Builder): flatbuffers.Offset;
  static createOk(
    builder: flatbuffers.Builder,
    messageOffset: flatbuffers.Offset
  ): flatbuffers.Offset;
  unpack(): OkT;
  unpackTo(_o: OkT): void;
}
export declare class OkT implements flatbuffers.IGeneratedObject {
  message: string | Uint8Array | null;
  constructor(message?: string | Uint8Array | null);
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
