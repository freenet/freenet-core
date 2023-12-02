import { Error } from "../topology/error";
import { Ok } from "../topology/ok";
export declare enum Response {
  NONE = 0,
  Error = 1,
  Ok = 2,
}
export declare function unionToResponse(
  type: Response,
  accessor: (obj: Error | Ok) => Error | Ok | null
): Error | Ok | null;
export declare function unionListToResponse(
  type: Response,
  accessor: (index: number, obj: Error | Ok) => Error | Ok | null,
  index: number
): Error | Ok | null;
