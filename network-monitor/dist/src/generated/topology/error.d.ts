import * as flatbuffers from "flatbuffers";
export declare class Error implements flatbuffers.IUnpackableObject<ErrorT> {
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): Error;
  static getRootAsError(bb: flatbuffers.ByteBuffer, obj?: Error): Error;
  static getSizePrefixedRootAsError(
    bb: flatbuffers.ByteBuffer,
    obj?: Error
  ): Error;
  message(): string | null;
  message(optionalEncoding: flatbuffers.Encoding): string | Uint8Array | null;
  static startError(builder: flatbuffers.Builder): void;
  static addMessage(
    builder: flatbuffers.Builder,
    messageOffset: flatbuffers.Offset
  ): void;
  static endError(builder: flatbuffers.Builder): flatbuffers.Offset;
  static createError(
    builder: flatbuffers.Builder,
    messageOffset: flatbuffers.Offset
  ): flatbuffers.Offset;
  unpack(): ErrorT;
  unpackTo(_o: ErrorT): void;
}
export declare class ErrorT implements flatbuffers.IGeneratedObject {
  message: string | Uint8Array | null;
  constructor(message?: string | Uint8Array | null);
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
