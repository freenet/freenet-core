import * as flatbuffers from "flatbuffers";
import { ErrorT } from "../topology/error";
import { OkT } from "../topology/ok";
import { Response } from "../topology/response";
export declare class ControllerResponse
  implements flatbuffers.IUnpackableObject<ControllerResponseT>
{
  bb: flatbuffers.ByteBuffer | null;
  bb_pos: number;
  __init(i: number, bb: flatbuffers.ByteBuffer): ControllerResponse;
  static getRootAsControllerResponse(
    bb: flatbuffers.ByteBuffer,
    obj?: ControllerResponse
  ): ControllerResponse;
  static getSizePrefixedRootAsControllerResponse(
    bb: flatbuffers.ByteBuffer,
    obj?: ControllerResponse
  ): ControllerResponse;
  responseType(): Response;
  response<T extends flatbuffers.Table>(obj: any): any | null;
  static startControllerResponse(builder: flatbuffers.Builder): void;
  static addResponseType(
    builder: flatbuffers.Builder,
    responseType: Response
  ): void;
  static addResponse(
    builder: flatbuffers.Builder,
    responseOffset: flatbuffers.Offset
  ): void;
  static endControllerResponse(
    builder: flatbuffers.Builder
  ): flatbuffers.Offset;
  static createControllerResponse(
    builder: flatbuffers.Builder,
    responseType: Response,
    responseOffset: flatbuffers.Offset
  ): flatbuffers.Offset;
  unpack(): ControllerResponseT;
  unpackTo(_o: ControllerResponseT): void;
}
export declare class ControllerResponseT
  implements flatbuffers.IGeneratedObject
{
  responseType: Response;
  response: ErrorT | OkT | null;
  constructor(responseType?: Response, response?: ErrorT | OkT | null);
  pack(builder: flatbuffers.Builder): flatbuffers.Offset;
}
