import { decode, Encoder } from "@msgpack/msgpack";
import base58 from "bs58";

const MAX_U8: number = 255;
const MIN_U8: number = 0;

// base interface types:

/**
 * The id of a live instance of a contract. This is effectively the tuple
 * of the hash of the hash of the contract code and a set of parameters used to run
 * the contract.
 * @public
 */
export type ContractInstanceId = Uint8Array;

/**
 * The key representing the tuple of a contract code and a set of parameters.
 * See {@link ContractInstanceId} for more information.
 * @public
 */
export class Key {
  private instance: ContractInstanceId;
  private code: Uint8Array | null;

  /**
   * @constructor
   * @param {ContractInstanceId} instance
   * @param {Uint8Array} [code]
   */
  constructor(instance: ContractInstanceId, code?: Uint8Array) {
    if (
      instance.length != 32 ||
      (typeof code != "undefined" && code.length != 32)
    ) {
      throw TypeError(
        "invalid array lenth (expected 32 bytes): " + instance.length
      );
    }
    this.instance = instance;
    if (typeof code == "undefined") {
      this.code = null;
    } else {
      this.code = code;
    }
  }

  /**
   * Generates key from base58 key spec representation
   * @example
   * Here's a simple example:
   * ```
   * const MODEL_CONTRACT = "DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1";
   * const KEY = Key.fromSpec(MODEL_CONTRACT);
   * ```
   * @param spec - Base58 string representation of the key
   * @returns The key representation from given spec
   * @constructor
   */
  static fromInstanceId(spec: string): Key {
    let encoded = base58.decode(spec);
    return new Key(encoded);
  }

  /**
   * @returns {ContractInstanceId} Hash of the full key specification (contract code + parameter).
   */
  bytes(): ContractInstanceId {
    return this.instance;
  }

  /**
   * @returns {Uint8Array | null} Hash of the contract code part of the full specification, if is available.
   */
  codePart(): Uint8Array | null {
    return this.code;
  }

  /**
   * Generates the full key specification (contract code + parameter) encoded as base58 string.
   *
   * @returns {string} The encoded string representation.
   */
  encode(): string {
    return base58.encode(this.instance);
  }
}

/**
 * A contract and its key. It includes the contract data/code and the parameters run along with it.
 * @public
 */
export type ContractV1 = {
  key: Key;
  data: Uint8Array;
  parameters: Uint8Array;
  version: String
};

/**
 * Representation of a contract state
 * @public
 */
export type State = Uint8Array;

/**
 * Representation of a contract state changes summary
 * @public
 */
export type StateSummary = Uint8Array;

/**
 * State delta representation
 * @public
 */
export type StateDelta = Uint8Array;

/** Update data from a notification for a contract which the client subscribed to.
 * It can be either the main contract or any related contracts to that main contract.
 * @public
 */
export type UpdateData =
  | { state: State }
  | { delta: StateDelta }
  | { state: State; delta: StateDelta }
  | { relatedTo: ContractInstanceId; state: State }
  | { relatedTo: ContractInstanceId; delta: StateDelta }
  | { relatedTo: ContractInstanceId; state: State; delta: StateDelta };

/**
 * A map of contract id's to their respective states in case this have
 * been successfully retrieved from the network.
 */
export type RelatedContracts = Map<ContractInstanceId, State | null>;

// ops:

/**
 * Representation of the client put request operation
 * @public
 */
export type PutRequest = {
  container: ContractContainer;
  state: State;
  relatedContracts: RelatedContracts;
};

/**
 * Representation of the client update request operation
 * @public
 */
export type UpdateRequest = {
  key: Key;
  data: UpdateData;
};

/**
 * Representation of the client get request operation
 * @public
 */
export type GetRequest = {
  key: Key;
  fetchContract: boolean;
};

/**
 * Representation of the client subscribe request operation
 * @public
 */
export type SubscribeRequest = {
  key: Key;
};

/**
 * Representation of the client disconnect request operation
 * @public
 */
export type DisconnectRequest = {
  cause?: string;
};

// API

/**
 * Interface to handle responses from the host
 *
 * @example
 * Here's a simple implementation example:
 * ```
 * const handler = {
 *  onPut: (_response: PutResponse) => {},
 *  onGet: (_response: GetResponse) => {},
 *  onUpdate: (_up: UpdateResponse) => {},
 *  onUpdateNotification: (_notif: UpdateNotification) => {},
 *  onErr: (err: HostError) => {},
 *  onOpen: () => {},
 * };
 * ```
 *
 * @public
 */
export interface ResponseHandler {
  /**
   * `Put` response handler
   */
  onPut: (response: PutResponse) => void;
  /**
   * `Get` response handler
   */
  onGet: (response: GetResponse) => void;
  /**
   * `Update` response handler
   */
  onUpdate: (response: UpdateResponse) => void;
  /**
   * `Update` notification handler
   */
  onUpdateNotification: (response: UpdateNotification) => void;
  /**
   * `Error` handler
   */
  onErr: (response: HostError) => void;
  /**
   * Callback executed after successfully establishing connection with websocket
   */
  onOpen: () => void;
}

/**
 * The `LocutusWsApi` provides the API to manage the connection to the host, handle responses, and send requests.
 * @example
 * Here's a simple example:
 * ```
 * const API_URL = new URL(`ws://${location.host}/contract/command/`);
 * const locutusApi = new LocutusWsApi(API_URL, handler);
 * ```
 */
export class LocutusWsApi {
  /**
   * Websocket object for creating and managing a WebSocket connection to a server,
   * as well as for sending and receiving data on the connection.
   * @private
   */
  private ws: WebSocket;
  /**
   * @private
   */
  private encoder: Encoder;
  /**
   * @private
   */
  private reponseHandler: ResponseHandler;

  /**
   * @constructor
   * @param url - The websocket URL to which to connect
   * @param handler - The ResponseHandler implementation
   */
  constructor(url: URL, handler: ResponseHandler) {
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";
    this.encoder = new Encoder();
    this.reponseHandler = handler;
    this.ws.onmessage = (ev) => {
      this.handleResponse(ev);
    };
    this.ws.addEventListener("open", (_) => {
      handler.onOpen();
    });
  }

  /**
   * @private
   */
  private handleResponse(ev: MessageEvent<any>): void | Error {
    let response;
    try {
      let data = new Uint8Array(ev.data);
      response = new HostResponse(data);
    } catch (err) {
      console.log(`found error: ${err}`);
      return new Error(`${err}`);
    }
    if (response.isOk()) {
      switch (response.unwrapOk().kind) {
        case "put":
          this.reponseHandler.onPut(response.unwrapPut());
        case "get":
          this.reponseHandler.onGet(response.unwrapGet());
        case "update":
          this.reponseHandler.onUpdate(response.unwrapUpdate());
        case "updateNotification":
          this.reponseHandler.onUpdateNotification(
            response.unwrapUpdateNotification()
          );
      }
    } else {
      this.reponseHandler.onErr(response.unwrapErr());
    }
  }

  /**
   * Sends a put request to the host through websocket
   * @param put - The `PutRequest` object
   */
  async put(put: PutRequest): Promise<void> {
    let encoded = this.encoder.encode(put);
    this.ws.send(encoded);
  }

  /**
   * Sends an update request to the host through websocket
   * @param update - The `UpdateRequest` object
   */
  async update(update: UpdateRequest): Promise<void> {
    let encoded = this.encoder.encode(update);
    this.ws.send(encoded);
  }

  /**
   * Sends a get request to the host through websocket
   * @param get - The `GetRequest` object
   */
  async get(get: GetRequest): Promise<void> {
    let encoded = this.encoder.encode(get);
    this.ws.send(encoded);
  }

  /**
   * Sends a subscribe request to the host through websocket
   * @param subscribe - The `SubscribeRequest` object
   */
  async subscribe(subscribe: SubscribeRequest): Promise<void> {
    let encoded = this.encoder.encode(subscribe);
    this.ws.send(encoded);
  }

  /**
   * Sends an disconnect notification to the host through websocket
   * @param disconnect - The `DisconnectRequest` object
   */
  async disconnect(disconnect: DisconnectRequest): Promise<void> {
    let encoded = this.encoder.encode(disconnect);
    this.ws.send(encoded);
    this.ws.close();
  }
}

// host replies:

/**
 * Host response types
 * @public
 */
export type Ok =
  | PutResponse
  | UpdateResponse
  | GetResponse
  | UpdateNotification;

/**
 * Host reponse error type
 * @public
 */
export type HostError = {
  cause: string;
};

/**
 * The response for a contract put operation
 * @public
 */
export interface PutResponse {
  readonly kind: "put";
  key: Key;
}

/**
 * The response for a contract update operation
 * @public
 */
export interface UpdateResponse {
  readonly kind: "update";
  key: Key;
  summary: StateSummary;
}

/**
 * The response for a contract get operation
 * @public
 */
export interface GetResponse {
  readonly kind: "get";
  contract?: ContractV1;
  state: State;
}

/**
 * The response for a state update notification
 * @public
 */
export interface UpdateNotification {
  readonly kind: "updateNotification";
  key: Key;
  update: UpdateData;
}

/**
 * Check that the condition is met
 * @param condition - Condition to check
 * @param [msg] - Error message
 * @public
 */
function assert(condition: boolean, msg?: string) {
  if (!condition) throw new TypeError(msg);
}

/**
 * A typed response from the host HTTP gateway to requests made via the API.
 *
 * @public
 */
export class HostResponse {
  /**
   * @private
   */
  private result: Ok | HostError;

  /**
   * Builds the response from the bytes received via the websocket interface.
   * @param bytes - Response data
   * @returns The corresponding response type result
   * @constructor
   */
  constructor(bytes: Uint8Array) {
    let decoded = decode(bytes) as object;
    if ("Ok" in decoded) {
      let ok = decoded as { Ok: any };
      if ("ContractResponse" in ok.Ok) {
        let response = ok.Ok as { ContractResponse: any };
        if ("PutResponse" in response.ContractResponse) {
          response.ContractResponse as { PutResponse: any };
          assert(Array.isArray(response.ContractResponse.PutResponse));
          let key = HostResponse.assertKey(response.ContractResponse.PutResponse[0][0]);
          this.result = { kind: "put", key };
          return;
        } else if ("UpdateResponse" in response.ContractResponse) {
          response.ContractResponse as { UpdateResponse: any };
          assert(Array.isArray(response.ContractResponse.UpdateResponse));
          assert(response.ContractResponse.UpdateResponse.length == 2);
          let key = HostResponse.assertKey(response.ContractResponse.UpdateResponse[0][0]);
          let summary = HostResponse.assertBytes(response.ContractResponse.UpdateResponse[1]);
          this.result = { kind: "update", key, summary };
          return;
        } else if ("GetResponse" in response.ContractResponse) {
          response.ContractResponse as { GetResponse: any };
          assert(Array.isArray(response.ContractResponse.GetResponse));
          assert(response.ContractResponse.GetResponse.length == 2);
          let contract;
          if (response.ContractResponse.GetResponse[0] !== null) {
            contract = {
              data: new Uint8Array(response.ContractResponse.GetResponse[0][0][1]),
              parameters: new Uint8Array(response.ContractResponse.GetResponse[0][1]),
              key: new Key(response.ContractResponse.GetResponse[0][2][0]),
            };
          } else {
            contract = null;
          }
          let get = {
            kind: "get",
            contract,
            state: response.ContractResponse.GetResponse[1],
          };
          this.result = get as GetResponse;
          return;
        } else if ("UpdateNotification" in response.ContractResponse) {
          response.ContractResponse as { UpdateNotification: any };
          assert(Array.isArray(response.ContractResponse.UpdateNotification));
          assert(response.ContractResponse.UpdateNotification.length == 2);
          let key = HostResponse.assertKey(response.ContractResponse.UpdateNotification[0][0]);
          let update = HostResponse.getUpdateData(response.ContractResponse.UpdateNotification[1]);
          this.result = {
            kind: "updateNotification",
            key,
            update,
          } as UpdateNotification;
          return;
        }
      }
    } else if ("Err" in decoded) {
      let err = decoded as { Err: Array<any> };
      if ("RequestError" in err.Err[0]) {
        function formatErr(kind: string, err: Array<any>): HostError {
          let contractKey = new Key(err[0][0]).encode();
          let cause =
            `${kind} error for contract ${contractKey}, cause: ` + err[1];
          return { cause };
        }

        if (typeof err.Err[0].RequestError === "string") {
          this.result = { cause: err.Err[0].RequestError };
          return;
        }
        if ("Put" in err.Err[0].RequestError) {
          let putErr = err.Err[0].RequestError.Put as Array<any>;
          this.result = formatErr("Put", putErr);
          return;
        } else if ("Update" in err.Err[0].RequestError) {
          let updateErr = err.Err[0].RequestError.Update as Array<any>;
          this.result = formatErr("Update", updateErr);
          return;
        } else if ("Get" in err.Err[0].RequestError) {
          let getErr = err.Err[0].RequestError.Get as Array<any>;
          this.result = formatErr("Get", getErr);
          return;
        } else if ("Disconnect" in err.Err[0].RequestError) {
          this.result = { cause: "client disconnected" };
          return;
        }
      }
    }
    throw new TypeError("bytes are not a valid HostResponse");
  }

  /**
   * Check if the response is ok or an error.
   * @returns True if contains the expected key otherwise false
   * @public
   */
  isOk(): boolean {
    if ("kind" in this.result) return true;
    else return false;
  }

  /**
   * Try to get the response content.
   * @returns The specific response content
   * @public
   */
  unwrapOk(): Ok {
    if ("kind" in this.result) {
      return this.result;
    } else throw new TypeError();
  }

  /**
   * Check if the response is an error.
   * @returns True if is an error otherwise false
   * @public
   */
  isErr(): boolean {
    if (this.result instanceof Error) return true;
    else return false;
  }

  /**
   * Get the specific error object from the response content
   * @returns The specific error
   * @public
   */
  unwrapErr(): HostError {
    if (this.result instanceof Error) return this.result as HostError;
    else throw new TypeError();
  }

  /**
   * Check if is a put response.
   * @returns True if is a put response otherwise false
   * @public
   */
  isPut(): boolean {
    return this.isOfType("put");
  }

  /**
   * Try to get the response content as a `PutResponse` object
   * @returns The PutResponse object
   * @public
   */
  unwrapPut(): PutResponse {
    if (this.isOfType("put")) return this.result as PutResponse;
    else throw new TypeError();
  }

  /**
   * Check if is an update response.
   * @returns True if is an update response otherwise false
   * @public
   */
  isUpdate(): boolean {
    return this.isOfType("update");
  }

  /**
   * Try to get the response content as an `UpdateResponse` object
   * @returns The UpdateResponse object
   * @public
   */
  unwrapUpdate(): UpdateResponse {
    if (this.isOfType("update")) return this.result as UpdateResponse;
    else throw new TypeError();
  }

  /**
   * Check if is a get response.
   * @returns True if is a get response otherwise false
   * @public
   */
  isGet(): boolean {
    return this.isOfType("get");
  }

  /**
   * Try to get the response content as a GetResponse object
   * @returns The GetResponse object
   * @public
   */
  unwrapGet(): GetResponse {
    if (this.isOfType("get")) return this.result as GetResponse;
    else throw new TypeError();
  }

  /**
   * Check if is a update notification response.
   * @returns True if is a update notification response otherwise false
   * @public
   */
  isUpdateNotification(): boolean {
    return this.isOfType("updateNotification");
  }

  /**
   * Try to get the response content as a UpdateNotification object
   * @returns The UpdateNotification object
   * @public
   */
  unwrapUpdateNotification(): UpdateNotification {
    if (this.isOfType("updateNotification"))
      return this.result as UpdateNotification;
    else throw new TypeError();
  }

  /**
   * @private
   */
  private isOfType(ty: string): boolean {
    return "kind" in this.result && this.result.kind === ty;
  }

  /**
   * @private
   */
  private static assertKey(key: any): Key {
    let bytes = HostResponse.assertBytes(key);
    assert(bytes.length === 32, "expected exactly 32 bytes");
    return new Key(bytes as Uint8Array);
  }

  /**
   * @private
   */
  private static assertBytes(state: any): Uint8Array {
    assert(Array.isArray(state));
    assert(
      state.every((value: any) => {
        if (typeof value === "number" && value >= MIN_U8 && value <= MAX_U8)
          return true;
        else return false;
      }),
      "expected an array of bytes"
    );
    return state as Uint8Array;
  }

  private static getUpdateData(update: UpdateData): UpdateData {
    if ("Delta" in update) {
      let delta = Array.from(update["Delta"]);
      return {
        delta: HostResponse.assertBytes(delta),
      };
    } else {
      throw new TypeError("Invalid update data while building HostResponse")
    }
  }
}

// Versioning:
type WasmContract = ContractV1

type ContractContainer = WasmContract;