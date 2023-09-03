// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

import { DelegateKey, DelegateKeyT } from '../client-request/delegate-key.js';
import { InboundDelegateMsg, InboundDelegateMsgT } from '../client-request/inbound-delegate-msg.js';


export class ApplicationMessages implements flatbuffers.IUnpackableObject<ApplicationMessagesT> {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):ApplicationMessages {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsApplicationMessages(bb:flatbuffers.ByteBuffer, obj?:ApplicationMessages):ApplicationMessages {
  return (obj || new ApplicationMessages()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsApplicationMessages(bb:flatbuffers.ByteBuffer, obj?:ApplicationMessages):ApplicationMessages {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new ApplicationMessages()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

key(obj?:DelegateKey):DelegateKey|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? (obj || new DelegateKey()).__init(this.bb!.__indirect(this.bb_pos + offset), this.bb!) : null;
}

params(index: number):number|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readUint8(this.bb!.__vector(this.bb_pos + offset) + index) : 0;
}

paramsLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

paramsArray():Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? new Uint8Array(this.bb!.bytes().buffer, this.bb!.bytes().byteOffset + this.bb!.__vector(this.bb_pos + offset), this.bb!.__vector_len(this.bb_pos + offset)) : null;
}

inbound(index: number, obj?:InboundDelegateMsg):InboundDelegateMsg|null {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? (obj || new InboundDelegateMsg()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

inboundLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startApplicationMessages(builder:flatbuffers.Builder) {
  builder.startObject(3);
}

static addKey(builder:flatbuffers.Builder, keyOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, keyOffset, 0);
}

static addParams(builder:flatbuffers.Builder, paramsOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, paramsOffset, 0);
}

static createParamsVector(builder:flatbuffers.Builder, data:number[]|Uint8Array):flatbuffers.Offset {
  builder.startVector(1, data.length, 1);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addInt8(data[i]!);
  }
  return builder.endVector();
}

static startParamsVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(1, numElems, 1);
}

static addInbound(builder:flatbuffers.Builder, inboundOffset:flatbuffers.Offset) {
  builder.addFieldOffset(2, inboundOffset, 0);
}

static createInboundVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startInboundVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endApplicationMessages(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  builder.requiredField(offset, 4) // key
  builder.requiredField(offset, 6) // params
  builder.requiredField(offset, 8) // inbound
  return offset;
}

static createApplicationMessages(builder:flatbuffers.Builder, keyOffset:flatbuffers.Offset, paramsOffset:flatbuffers.Offset, inboundOffset:flatbuffers.Offset):flatbuffers.Offset {
  ApplicationMessages.startApplicationMessages(builder);
  ApplicationMessages.addKey(builder, keyOffset);
  ApplicationMessages.addParams(builder, paramsOffset);
  ApplicationMessages.addInbound(builder, inboundOffset);
  return ApplicationMessages.endApplicationMessages(builder);
}

unpack(): ApplicationMessagesT {
  return new ApplicationMessagesT(
    (this.key() !== null ? this.key()!.unpack() : null),
    this.bb!.createScalarList<number>(this.params.bind(this), this.paramsLength()),
    this.bb!.createObjList<InboundDelegateMsg, InboundDelegateMsgT>(this.inbound.bind(this), this.inboundLength())
  );
}


unpackTo(_o: ApplicationMessagesT): void {
  _o.key = (this.key() !== null ? this.key()!.unpack() : null);
  _o.params = this.bb!.createScalarList<number>(this.params.bind(this), this.paramsLength());
  _o.inbound = this.bb!.createObjList<InboundDelegateMsg, InboundDelegateMsgT>(this.inbound.bind(this), this.inboundLength());
}
}

export class ApplicationMessagesT implements flatbuffers.IGeneratedObject {
constructor(
  public key: DelegateKeyT|null = null,
  public params: (number)[] = [],
  public inbound: (InboundDelegateMsgT)[] = []
){}


pack(builder:flatbuffers.Builder): flatbuffers.Offset {
  const key = (this.key !== null ? this.key!.pack(builder) : 0);
  const params = ApplicationMessages.createParamsVector(builder, this.params);
  const inbound = ApplicationMessages.createInboundVector(builder, builder.createObjectOffsetList(this.inbound));

  return ApplicationMessages.createApplicationMessages(builder,
    key,
    params,
    inbound
  );
}
}
