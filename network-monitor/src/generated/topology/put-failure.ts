// automatically generated by the FlatBuffers compiler, do not modify

import * as flatbuffers from 'flatbuffers';

export class PutFailure {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):PutFailure {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsPutFailure(bb:flatbuffers.ByteBuffer, obj?:PutFailure):PutFailure {
  return (obj || new PutFailure()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsPutFailure(bb:flatbuffers.ByteBuffer, obj?:PutFailure):PutFailure {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new PutFailure()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

transaction():string|null
transaction(optionalEncoding:flatbuffers.Encoding):string|Uint8Array|null
transaction(optionalEncoding?:any):string|Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.__string(this.bb_pos + offset, optionalEncoding) : null;
}

requester():string|null
requester(optionalEncoding:flatbuffers.Encoding):string|Uint8Array|null
requester(optionalEncoding?:any):string|Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__string(this.bb_pos + offset, optionalEncoding) : null;
}

target():string|null
target(optionalEncoding:flatbuffers.Encoding):string|Uint8Array|null
target(optionalEncoding?:any):string|Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? this.bb!.__string(this.bb_pos + offset, optionalEncoding) : null;
}

key():string|null
key(optionalEncoding:flatbuffers.Encoding):string|Uint8Array|null
key(optionalEncoding?:any):string|Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 10);
  return offset ? this.bb!.__string(this.bb_pos + offset, optionalEncoding) : null;
}

static startPutFailure(builder:flatbuffers.Builder) {
  builder.startObject(4);
}

static addTransaction(builder:flatbuffers.Builder, transactionOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, transactionOffset, 0);
}

static addRequester(builder:flatbuffers.Builder, requesterOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, requesterOffset, 0);
}

static addTarget(builder:flatbuffers.Builder, targetOffset:flatbuffers.Offset) {
  builder.addFieldOffset(2, targetOffset, 0);
}

static addKey(builder:flatbuffers.Builder, keyOffset:flatbuffers.Offset) {
  builder.addFieldOffset(3, keyOffset, 0);
}

static endPutFailure(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  builder.requiredField(offset, 4) // transaction
  builder.requiredField(offset, 6) // requester
  builder.requiredField(offset, 8) // target
  builder.requiredField(offset, 10) // key
  return offset;
}

static createPutFailure(builder:flatbuffers.Builder, transactionOffset:flatbuffers.Offset, requesterOffset:flatbuffers.Offset, targetOffset:flatbuffers.Offset, keyOffset:flatbuffers.Offset):flatbuffers.Offset {
  PutFailure.startPutFailure(builder);
  PutFailure.addTransaction(builder, transactionOffset);
  PutFailure.addRequester(builder, requesterOffset);
  PutFailure.addTarget(builder, targetOffset);
  PutFailure.addKey(builder, keyOffset);
  return PutFailure.endPutFailure(builder);
}
}
