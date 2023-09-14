import * as flatbuffers from "flatbuffers";
import base58 from "bs58";

import { ContractContainerT } from "./common/contract-container";
import { ContractInstanceIdT } from "./common/contract-instance-id";
import { DeltaUpdateT } from "./common/delta-update";
import { RelatedDeltaUpdateT } from "./common/related-delta-update";
import { RelatedStateAndDeltaUpdateT } from "./common/related-state-and-delta-update";
import { RelatedStateUpdateT } from "./common/related-state-update";
import { StateAndDeltaUpdateT } from "./common/state-and-delta-update";
import { StateUpdateT } from "./common/state-update";
import {
  ApplicationMessagesT,
  AuthenticateT,
  ClientRequest,
  ClientRequestT,
  ClientRequestType,
  ContractRequestT,
  ContractRequestType,
  DelegateCodeT,
  DelegateContainerT,
  DelegateKeyT,
  DelegateRequestT,
  DelegateRequestType,
  DelegateType,
  DisconnectT,
  GetSecretRequestTypeT,
  GetT,
  InboundDelegateMsgT,
  InboundDelegateMsgType,
  PutT,
  RandomBytesT,
  RegisterDelegateT,
  RelatedContractsT,
  SubscribeT,
  UnregisterDelegateT,
  UpdateT,
  UserInputResponseT,
  WasmDelegateV1T,
} from "./client-request";
import { UpdateDataT } from "./common/update-data";
import { UpdateDataType } from "./common/update-data-type";
import { ContractKeyT } from "./common/contract-key";
import { PutResponseT } from "./host-response/put-response";
import { GetResponseT } from "./host-response/get-response";
import { UpdateResponseT } from "./host-response/update-response";
import { UpdateNotificationT } from "./host-response/update-notification";
import { HostResponse, HostResponseT } from "./host-response/host-response";
import {
  ContextUpdatedT,
  ContractResponseT,
  ContractResponseType,
  DelegateResponseT,
  HostResponseType,
  OutboundDelegateMsgT,
  OutboundDelegateMsgType,
  RandomBytesRequestT,
  RequestUserInputT,
  SetSecretRequestT,
} from "./host-response";
import {
  ApplicationMessageT,
  ContractCodeT,
  ContractType,
  GetSecretRequestT,
  GetSecretResponseT,
  WasmContractV1T,
} from "./common";
import { ErrorT } from "./host-response/error";

// Common types
/**
 * The id of a live instance of a contract. This is effectively the tuple
 * of the hash of the hash of the contract code and a set of parameters used to run
 * the contract.
 * @public
 */
export type ContractInstanceId = Uint8Array;

/**
 * Update notifications for a contract or a related contract.
 * @public
 */
export class UpdateData extends UpdateDataT {
  constructor(
    updateDataType: UpdateDataType = UpdateDataType.NONE,
    updateData:
      | DeltaUpdateT
      | RelatedDeltaUpdateT
      | RelatedStateAndDeltaUpdateT
      | RelatedStateUpdateT
      | StateAndDeltaUpdateT
      | StateUpdateT
      | null = null
  ) {
    super(updateDataType, updateData);
  }
}

/**
 * Representation of the state update data
 * @public
 */
export class StateUpdate extends StateUpdateT {
  constructor(state: number[] = []) {
    super(state);
  }
}

/**
 * Representation of the delta update data
 * @public
 */
export class DeltaUpdate extends DeltaUpdateT {
  constructor(delta: number[] = []) {
    super(delta);
  }
}

/**
 * Representation of the state and delta update data
 * @public
 */
export class StateAndDeltaUpdate extends StateAndDeltaUpdateT {
  constructor(state: number[] = [], delta: number[] = []) {
    super(state, delta);
  }
}

/**
 * Representation of the related state update data
 * @public
 */
export class RelatedStateUpdate extends RelatedStateUpdateT {
  constructor(
    relatedTo: ContractInstanceIdT | null = null,
    state: number[] = []
  ) {
    super(relatedTo, state);
  }
}

/**
 * Representation of the related delta update data
 * @public
 */
export class RelatedDeltaUpdate extends RelatedDeltaUpdateT {
  constructor(
    relatedTo: ContractInstanceIdT | null = null,
    delta: number[] = []
  ) {
    super(relatedTo, delta);
  }
}

/**
 * Representation of the related state and delta update data
 * @public
 */
export class RelatedStateAndDeltaUpdate extends RelatedStateAndDeltaUpdateT {
  constructor(
    relatedTo: ContractInstanceIdT | null = null,
    state: number[] = [],
    delta: number[] = []
  ) {
    super(relatedTo, state, delta);
  }
}

/**
 * Representation of the ContractKey
 * @public
 */
export class ContractKey extends ContractKeyT {
  constructor(instance: ContractInstanceId, code?: Uint8Array) {
    if (instance.length !== 32 || (code && code.length !== 32)) {
      throw new TypeError("Invalid array length, expected 32 bytes");
    }

    let contract_instance_id = new ContractInstanceIdT(Array.from(instance));
    let contract_code: number[] = [];
    if (code) {
      contract_code = Array.from(code);
    }
    super(contract_instance_id, contract_code);
  }

  static fromInstanceId(spec: string): ContractKey {
    const decoded = base58.decode(spec);
    return new ContractKey(decoded);
  }

  bytes(): ContractInstanceId {
    return new Uint8Array(this.instance?.data!) as ContractInstanceId;
  }

  codePart(): Uint8Array | null {
    return new Uint8Array(this.code);
  }

  encode(): string {
    const instance = new Uint8Array(this.instance?.data!);
    return base58.encode(instance);
  }

  get_contract_key(): ContractKey {
    return this;
  }
}

/**
 * Representation of the DelegateKey
 * @public
 */
export type DelegateKey = DelegateKeyT;

/**
 * Representation of the ContractCode
 * @public
 */
export type ContractCode = ContractCodeT;

/**
 * Representation of the DelegateCode
 * @public
 */
export type DelegateCode = DelegateCodeT;

/**
 * Representation of the WasmContractV1
 * @public
 */
export class WasmContractV1 extends WasmContractV1T {
  constructor(
    data: ContractCode | null = null,
    parameters: number[] = [],
    key: ContractKeyT | null = null
  ) {
    super(data, parameters, key);
  }
}

/**
 * Contract version type
 */
export type Contract = WasmContractV1;

/**
 * Wrapper that allows contract versioning. This enum maintains the types of contracts that are allowed
 * and their corresponding version.
 * @public
 */
export class ContractContainer extends ContractContainerT {
  constructor(
    contractType: ContractType = ContractType.NONE,
    contract: Contract
  ) {
    super(contractType, contract);
  }
}

/**
 * Representation of the WasmDelegateV1
 * @public
 */
export class WasmDelegateV1 extends WasmDelegateV1T {
  constructor(parameters: number[] = [], data: DelegateCode, key: DelegateKey) {
    super(parameters, data, key);
  }
}

/**
 * Delegate version type
 */
export type Delegate = WasmDelegateV1;

/**
 * Wrapper that allows delegate versioning. This enum maintains the types of delegates that are allowed
 * and their corresponding version.
 * @public
 */
export class DelegateContainer extends DelegateContainerT {
  constructor(
    delegateType: DelegateType = DelegateType.NONE,
    delegate: Delegate
  ) {
    super(delegateType, delegate);
  }
}

/**
 * Representation of the delegate GetSecretRequest message content
 * @public
 */
export type GetSecretRequest = GetSecretRequestT;
/**
 * Representation of the delegate GetSecretRequest message type
 * @public
 */
export type GetSecretRequestType = GetSecretRequestTypeT;
/**
 * Representation of the delegate GetSecretResponse message content
 * @public
 */
export type GetSecretResponse = GetSecretResponseT;

/**
 * Representation of the delegate Application message
 *
 */
export type ApplicationMessage = ApplicationMessageT;

// Client requests

// Contract

/**
 * Representation of the client put request operation
 * @public
 */
export class PutRequest extends PutT {
  constructor(
    container: ContractContainerT | null = null,
    wrappedState: number[] = [],
    relatedContracts: RelatedContractsT | null = null
  ) {
    super(container, wrappedState, relatedContracts);
  }
}

/**
 * Representation of the client update request operation
 * @public
 */
export class UpdateRequest extends UpdateT {
  constructor(
    key: ContractKey | null = null,
    update: UpdateDataT | null = null
  ) {
    const contract_key = key?.get_contract_key();
    super(contract_key, update);
  }
}

/**
 * Representation of the client get request operation
 * @public
 */
export class GetRequest extends GetT {
  constructor(key: ContractKey, fetchContract: boolean = false) {
    const contract_key = key.get_contract_key();
    super(contract_key, fetchContract);
  }
}

/**
 * Representation of the client subscribe request operation
 * @public
 */
export class SubscribeRequest extends SubscribeT {
  constructor(key: ContractKey | null = null, summary: number[] = []) {
    const contract_key = key?.get_contract_key();
    super(contract_key, summary);
  }
}

/**
 * Representation of the client disconnect request operation
 * @public
 */
export class DisconnectRequest extends DisconnectT {
  constructor(cause: string | Uint8Array | null = null) {
    super(cause);
  }
}

// Delegate
/**
 * Representation of the RandomBytes message
 * @public
 */
export type RandomBytes = RandomBytesT;
/**
 * Representation of the UserInputResponse message
 * @public
 */
export type UserInputResponse = UserInputResponseT;

export type InboundMessage =
  | ApplicationMessage
  | GetSecretResponse
  | RandomBytes
  | UserInputResponse
  | GetSecretRequest;

/**
 * Representation of DelegateRequest Inbound message
 * @public
 */
export class InboundDelegateMsg extends InboundDelegateMsgT {
  constructor(
    inboundType: InboundDelegateMsgType = InboundDelegateMsgType.NONE,
    inbound: InboundMessage
  ) {
    super(inboundType, inbound);
  }
}
/**
 * Representation of an inbound application messages
 * @public
 */
export type ApplicationMessages = ApplicationMessagesT;
/**
 * Representation of the RegisterDelegate message
 * @public
 */
export type RegisterDelegate = RegisterDelegateT;
/**
 * Representation of the UnregisterDelegate message
 * @public
 */
export type UnregisterDelegate = UnregisterDelegateT;

export class DelegateRequest extends DelegateRequestT {
  constructor(
    delegateRequestType: DelegateRequestType = DelegateRequestType.NONE,
    delegateRequest:
      | ApplicationMessages
      | GetSecretRequestType
      | RegisterDelegate
      | UnregisterDelegate
  ) {
    super(delegateRequestType, delegateRequest);
  }
}

// Host replies

// Contract
/**
 * The response for a contract put operation
 * @public
 */
export class PutResponse extends PutResponseT {
    constructor(public key: ContractKey) {
        super(key);
    }

    static fromPutResponseT(obj: PutResponseT): PutResponse {
        // Build the contract key
        let instance = new Uint8Array(obj.key?.instance?.data!);
        const code = obj.key?.code && obj.key.code.length > 0 ? new Uint8Array(obj.key.code!) : undefined;
        let key: ContractKey = new ContractKey(instance, code);

        return new PutResponse(key);
    }
}

/**
 * The response for a contract get operation
 * @public
 */
export class GetResponse extends GetResponseT {
    constructor(public key: ContractKey, public contract: ContractContainer, public state: (number)[] = []) {
        super(key, contract, state);
    }

    static fromGetResponseT(obj: GetResponseT): GetResponse {
        // Build the contract key
        let instance = new Uint8Array(obj.key?.instance?.data!);
        const code = obj.key?.code && obj.key.code.length > 0 ? new Uint8Array(obj.key.code!) : undefined;
        let key: ContractKey = new ContractKey(instance, code);

        return new GetResponse(key, obj.contract!, obj.state);
    }
}

/**
 * The response for a contract update operation
 * @public
 */
export class UpdateResponse extends UpdateResponseT {
    constructor(public key: ContractKey, public summary: (number)[] = []) {
        super(key, summary);
    }

    static fromUpdateResponseT(obj: UpdateResponseT): UpdateResponse {
        // Build the contract key
        let instance = new Uint8Array(obj.key?.instance?.data!);
        const code = obj.key?.code && obj.key.code.length > 0 ? new Uint8Array(obj.key.code!) : undefined;
        let key: ContractKey = new ContractKey(instance, code);

        return new UpdateResponse(key, obj.summary);
    }
}

/**
 * The response for a contract update notification
 * @public
 */
export class UpdateNotification extends UpdateNotificationT {
    constructor(public key: ContractKey, public update: UpdateData) {
        super(key, update);
    }

    static fromUpdateNotificationT(obj: UpdateNotificationT): UpdateNotification {
        // Build the contract key
        let instance = new Uint8Array(obj.key?.instance?.data!);
        const code = obj.key?.code && obj.key.code.length > 0 ? new Uint8Array(obj.key.code!) : undefined;
        let key: ContractKey = new ContractKey(instance);


        return new UpdateNotification(key, obj.update!);
    }
}

// Delegate
/**
 * Representation of ContextUpdated message
 * @public
 */
export type ContextUpdated = ContextUpdatedT;
/**
 * Representation of RandomBytesRequest message
 * @public
 */
export type RandomBytesRequest = RandomBytesRequestT;
/**
 * Representation of RequestUserInput message
 * @public
 */
export type RequestUserInput = RequestUserInputT;
/**
 * Representation of GetSecretRequest message
 * @public
 */
export type SetSecretRequest = SetSecretRequestT;

/**
 * Representation of the outbound delegate message types
 * @public
 */
export type OutboundMessage =
  | ApplicationMessage
  | RequestUserInput
  | ContextUpdated
  | GetSecretRequest
  | SetSecretRequest
  | RandomBytesRequest
  | GetSecretResponse;

export class OutboundDelegateMsg extends OutboundDelegateMsgT {
  constructor(
    inboundType: OutboundDelegateMsgType = OutboundDelegateMsgType.NONE,
    inbound: OutboundMessage
  ) {
    super(inboundType, inbound);
  }
}

/**
 * The response for a delegate operation
 * @public
 */
export class DelegateResponse extends DelegateResponseT {
  constructor(
    key: DelegateKey | null = null,
    values: OutboundDelegateMsg[] = []
  ) {
    super(key, values);
  }
}

/**
 * Host response error type
 * @public
 */
export type HostError = {
  cause: string;
};

// API

/**
 * Interface to handle responses from the host
 *
 * @example
 * Here's a simple implementation example:
 * ```
 * const handler = {
 *  onContractPut: (_response: PutResponse) => {},
 *  onContractGet: (_response: GetResponse) => {},
 *  onContractUpdate: (_up: UpdateResponse) => {},
 *  onContractUpdateNotification: (_notif: UpdateNotification) => {},
 *  onDelegateResponse: (_response: DelegateResponse) => {},
 *  onErr: (err: HostError) => {},
 *  onOpen: () => {},
 * };
 * ```
 *
 * @public
 */
export interface ResponseHandler {
  /**
   * Contract `Put` response handler
   */
  onContractPut: (response: PutResponse) => void;
  /**
   * Contract `Get` response handler
   */
  onContractGet: (response: GetResponse) => void;
  /**
   * Contract `Update` response handler
   */
  onContractUpdate: (response: UpdateResponse) => void;
  /**
   * Contract `Update` notification handler
   */
  onContractUpdateNotification: (response: UpdateNotification) => void;
  /**
   * `Delegate` response handler
   * @param response
   */
  onDelegateResponse: (response: DelegateResponse) => void;
  /**
   * Contract `Error` handler
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

const AUTHORIZATION_HEADER: string = "Authorization";
const ENCODING_PROTOC_HEADER: string = `Encoding-Protocol`;
const ENCODING_PROTOC: string = "flatbuffers";

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
  private responseHandler: ResponseHandler;

  /**
   * @constructor
   * @param url - The websocket URL to establish the connection.
   * @param handler - The ResponseHandler implementation
   */
  constructor(url: URL, handler: ResponseHandler, authToken?: string) {
    // let protocols = [`${ENCODING_PROTOC_HEADER}: ${ENCODING_PROTOC}`];
    if (authToken) {
      url.searchParams.append("authToken", authToken);
      //   protocols.push(`${AUTHORIZATION_HEADER}: Bearer ${authToken}`);
    }
    url.searchParams.append("encodingProtocol", ENCODING_PROTOC);
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";
    this.responseHandler = handler;
    this.ws.onmessage = (ev) => {
      this.handleResponse(ev);
    };
    this.ws.addEventListener("open", (_) => {
      if (authToken) {
        const request = new ClientRequestT(
          ClientRequestType.Authenticate,
          new AuthenticateT(authToken!)
        );
        const fbb = new flatbuffers.Builder();
        ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
        this.ws.send(fbb.asUint8Array());
      }
      handler.onOpen();
    });
  }

    /**
     * @private
     */
    private handleResponse(ev: MessageEvent<any>): void | Error {
        let response: HostResponseT;
        try {
            let data = new flatbuffers.ByteBuffer(new Uint8Array(ev.data));
            response = HostResponse.getRootAsHostResponse(data).unpack();
        } catch (err) {
            console.log(`found error: ${err}`);
            return new Error(`${err}`);
        }
        switch (response.responseType) {
            case HostResponseType.ContractResponse:
                let host_resp = response.response as ContractResponseT;
                switch (host_resp.contractResponseType) {
                    case ContractResponseType.PutResponse:
                        const put_response = PutResponse.fromPutResponseT(host_resp.contractResponse as PutResponseT)
                        this.responseHandler.onContractPut(put_response);
                        break;
                    case ContractResponseType.GetResponse:
                        const get_response = GetResponse.fromGetResponseT(host_resp.contractResponse as GetResponseT);
                        this.responseHandler.onContractGet(get_response);
                        break;
                    case ContractResponseType.UpdateResponse:
                        const update_response = UpdateResponse.fromUpdateResponseT(host_resp.contractResponse as UpdateResponseT);
                        this.responseHandler.onContractUpdate(update_response);
                        break;
                    case ContractResponseType.UpdateNotification:
                        const update_notification = UpdateNotification.fromUpdateNotificationT(host_resp.contractResponse as UpdateNotificationT);
                        this.responseHandler.onContractUpdateNotification(update_notification);
                        break;
                    default:
                        const cause = "Contract response type not implemented";
                        console.log(cause);
                        const err: HostError = {
                            cause,
                        }
                        this.responseHandler.onErr(err);
                        break;

                }
                break;
            case HostResponseType.DelegateResponse:
                let delegate_response = response.response as DelegateResponseT;
                this.responseHandler.onDelegateResponse(delegate_response);
                break;
            case HostResponseType.Ok:
                break;
            default:
                const cause = `Received wrong HostResponse type`;
                console.log(cause);
                const err: HostError = {
                    cause,
                }
                this.responseHandler.onErr(err);
                break;
        }
    }

  /**
   * Sends a put request to the host through websocket
   * @param put - The `PutRequest` object
   */
  async put(put: PutRequest): Promise<void> {
    let put_request = new ContractRequestT(ContractRequestType.Put, put);
    let request = new ClientRequestT(
      ClientRequestType.ContractRequest,
      put_request
    );
    let fbb = new flatbuffers.Builder(1024);
    ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
    this.ws.send(fbb.asUint8Array());
  }

  /**
   * Sends an update request to the host through websocket
   * @param update - The `UpdateRequest` object
   */
  async update(update: UpdateRequest): Promise<void> {
    let update_request = new ContractRequestT(
      ContractRequestType.Update,
      update
    );
    let request = new ClientRequestT(
      ClientRequestType.ContractRequest,
      update_request
    );
    let fbb = new flatbuffers.Builder(1024);
    ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
    this.ws.send(fbb.asUint8Array());
  }

  /**
   * Sends a get request to the host through websocket
   * @param get - The `GetRequest` object
   */
  async get(get: GetRequest): Promise<void> {
    let get_request = new ContractRequestT(ContractRequestType.Get, get);
    let request = new ClientRequestT(
      ClientRequestType.ContractRequest,
      get_request
    );
    let fbb = new flatbuffers.Builder(1024);
    ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
    this.ws.send(fbb.asUint8Array());
  }

  /**
   * Sends a subscribe request to the host through websocket
   * @param subscribe - The `SubscribeRequest` object
   */
  async subscribe(subscribe: SubscribeRequest): Promise<void> {
    let subscribe_request = new ContractRequestT(
      ContractRequestType.Subscribe,
      subscribe
    );
    let request = new ClientRequestT(
      ClientRequestType.ContractRequest,
      subscribe_request
    );
    let fbb = new flatbuffers.Builder(1024);
    ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
    this.ws.send(fbb.asUint8Array());
  }

  /**
   * Sends an disconnect notification to the host through websocket
   * @param disconnect - The `DisconnectRequest` object
   */
  async disconnect(disconnect: DisconnectRequest): Promise<void> {
    let request = new ClientRequestT(ClientRequestType.Disconnect, disconnect);
    let fbb = new flatbuffers.Builder(1024);
    ClientRequest.finishClientRequestBuffer(fbb, request.pack(fbb));
    this.ws.send(fbb.asUint8Array());
  }
}
