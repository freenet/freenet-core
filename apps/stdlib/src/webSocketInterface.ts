import flatbuffers from 'flatbuffers';
import base58 from "bs58";

import {client_request, common} from "./client_request";
import {host_response} from "./host_response";
import {ContractContainerT} from "./common/contract-container";
import {ContractInstanceIdT} from "./common/contract-instance-id";
import {DeltaUpdateT} from "./common/delta-update";
import {RelatedDeltaUpdateT} from "./common/related-delta-update";
import {RelatedStateAndDeltaUpdateT} from "./common/related-state-and-delta-update";
import {RelatedStateUpdateT} from "./common/related-state-update";
import {StateAndDeltaUpdateT} from "./common/state-and-delta-update";
import {StateUpdateT} from "./common/state-update";

/**
 * The id of a live instance of a contract. This is effectively the tuple
 * of the hash of the hash of the contract code and a set of parameters used to run
 * the contract.
 * @public
 */
export type ContractInstanceId = Uint8Array;


/**
 * Representation of the client put request operation
 * @public
 */
export class PutRequest extends client_request.PutT {
    constructor(container: ContractContainerT | null = null,
                wrappedState: (number)[] = [],
                relatedContracts: client_request.RelatedContractsT | null = null) {
        super(container, wrappedState, relatedContracts);
    }
}

/**
 * Representation of the client update request operation
 * @public
 */
export class UpdateRequest extends client_request.UpdateT {
    constructor(key: Key | null = null, update: common.UpdateDataT | null = null) {
        const contract_key = key?.get_contract_key().unpack();
        super(contract_key, update);
    }
}

/**
 * Representation of the client get request operation
 * @public
 */
export class GetRequest extends client_request.GetT {
    constructor(key: Key | null = null, fetchContract: boolean = false) {
        const contract_key = key?.get_contract_key().unpack();
        super(contract_key, fetchContract);
    }
}

/**
 * Representation of the client subscribe request operation
 * @public
 */
export class SubscribeRequest extends client_request.SubscribeT {
    constructor(key: Key | null = null, summary: (number)[] = []) {
        const contract_key = key?.get_contract_key().unpack();
        super(contract_key, summary);
    }
}

/**
 * Representation of the client disconnect request operation
 * @public
 */
export class DisconnectRequest extends client_request.DisconnectT {
    constructor(cause: string | Uint8Array | null = null) {
        super(cause);
    }
}

export class UpdateData extends common.UpdateDataT {
    constructor(updateDataType: common.UpdateDataType = common.UpdateDataType.NONE,
                updateData: DeltaUpdateT | RelatedDeltaUpdateT | RelatedStateAndDeltaUpdateT | RelatedStateUpdateT
                    | StateAndDeltaUpdateT | StateUpdateT | null = null) {
        super(updateDataType, updateData);
    }
}

/**
 * Representation of the state update data
 * @public
 */
export class StateUpdate extends common.StateUpdateT {
    constructor(state: (number)[] = []) {
        super(state);
    }
}

/**
 * Representation of the delta update data
 * @public
 */
export class DeltaUpdate extends common.DeltaUpdateT {
    constructor(delta: (number)[] = []) {
        super(delta);
    }
}

/**
 * Representation of the state and delta update data
 * @public
 */
export class StateAndDeltaUpdate extends common.StateAndDeltaUpdateT {
    constructor(state: (number)[] = [], delta: (number)[] = []) {
        super(state, delta);
    }
}

/**
 * Representation of the related state update data
 * @public
 */
export class RelatedStateUpdate extends common.RelatedStateUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, state: (number)[] = []) {
        super(relatedTo, state);
    }
}

/**
 * Representation of the related delta update data
 * @public
 */
export class RelatedDeltaUpdate extends common.RelatedDeltaUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, delta: (number)[] = []) {
        super(relatedTo, delta);
    }
}

/**
 * Representation of the related state and delta update data
 * @public
 */
export class RelatedStateAndDeltaUpdate extends common.RelatedStateAndDeltaUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, state: (number)[] = [], delta: (number)[] = []) {
        super(relatedTo, state, delta);
    }
}


export class Key {
    private builder = new flatbuffers.Builder(1024);
    private key: common.ContractKey;

    constructor(instance: ContractInstanceId, code?: Uint8Array) {
        if (instance.length !== 32 || (code && code.length !== 32)) {
            throw new TypeError('Invalid array length, expected 32 bytes');
        }

        const instanceOffset = common.ContractInstanceId.createDataVector(this.builder, instance);
        const instanceEndOffset = common.ContractInstanceId.createContractInstanceId(this.builder, instanceOffset);
        common.ContractKey.startContractKey(this.builder);
        common.ContractKey.addInstance(this.builder, instanceEndOffset);
        if (code) {
            const codeOffset = common.ContractKey.createCodeVector(this.builder, code);
            common.ContractKey.addCode(this.builder, codeOffset);
        }
        const keyOffset = common.ContractKey.endContractKey(this.builder);
        this.builder.finish(keyOffset);
        this.key = common.ContractKey.getRootAsContractKey(this.builder.dataBuffer());
    }

    static fromInstanceId(spec: string): Key {
        const decoded = base58.decode(spec);
        return new Key(decoded);
    }

    bytes(): ContractInstanceId {
        return this.key.instance()?.dataArray()!;
    }

    codePart(): Uint8Array | null {
        return this.key.codeArray();
    }

    encode(): string {
        const instance = this.key.instance()?.dataArray()!;
        return base58.encode(instance);
    }

    get_contract_key(): common.ContractKey {
        return this.key;
    }
}


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
    onPut: (response: host_response.PutResponseT) => void;
    /**
     * `Get` response handler
     */
    onGet: (response: host_response.GetResponseT) => void;
    /**
     * `Update` response handler
     */
    onUpdate: (response: host_response.UpdateResponseT) => void;
    /**
     * `Update` notification handler
     */
    onUpdateNotification: (response: host_response.UpdateNotificationT) => void;
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
    private reponseHandler: ResponseHandler;

    /**
     * @constructor
     * @param url - The websocket URL to which to connect
     * @param handler - The ResponseHandler implementation
     */
    constructor(url: URL, handler: ResponseHandler) {
        this.ws = new WebSocket(url);
        this.ws.binaryType = "arraybuffer";
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
        let response: host_response.HostResponseT;
        try {
            let data = new flatbuffers.ByteBuffer(ev.data);
            response = new host_response.HostResponseT();
            host_response.HostResponse.getRootAsHostResponse(data).unpackTo(response);
        } catch (err) {
            console.log(`found error: ${err}`);
            return new Error(`${err}`);
        }
        switch (response.responseType) {
            case host_response.HostResponseType.ContractResponse:
                let host_resp = response.response as host_response.ContractResponseT;
                switch (host_resp.contractResponseType) {
                    case host_response.ContractResponseType.PutResponse:
                        const put_response = host_resp.contractResponse as host_response.PutResponseT;
                        this.reponseHandler.onPut(put_response);
                        break;
                    case host_response.ContractResponseType.GetResponse:
                        const get_response = host_resp.contractResponse as host_response.GetResponseT;
                        this.reponseHandler.onGet(get_response);
                        break;
                    case host_response.ContractResponseType.UpdateResponse:
                        const update_response = host_resp.contractResponse as host_response.UpdateResponseT;
                        this.reponseHandler.onUpdate(update_response);
                        break;
                    case host_response.ContractResponseType.UpdateNotification:
                        const update_notification = host_resp.contractResponse as host_response.UpdateNotificationT;
                        this.reponseHandler.onUpdateNotification(update_notification);
                        break;
                    default:
                        const cause = `Contract response type not implemented`;
                        console.log(cause);
                        const err: HostError = {
                            cause,
                        }
                        this.reponseHandler.onErr(err);
                        break;

                }
                break;
            case host_response.HostResponseType.DelegateResponse:
                console.log(`Delegate response handler not implemented`);
                break;
            case host_response.HostResponseType.GenerateRandData:
                console.log(`GenerateRandData response handler not implemented`);
                break;
            case host_response.HostResponseType.Ok:
                console.log(`not implemented`);
                break;
            case host_response.HostResponseType.NONE:
                console.log(`response error`);
                break;
            default:
                const cause = `HostResponse type not implemented`;
                console.log(cause);
                const err: HostError = {
                    cause,
                }
                this.reponseHandler.onErr(err);
                break;
        }
    }

    /**
     * Sends a put request to the host through websocket
     * @param put - The `PutRequest` object
     */
    async put(put: PutRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        client_request.ClientRequest.finishClientRequestBuffer(fbb, put.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends an update request to the host through websocket
     * @param update - The `UpdateRequest` object
     */
    async update(update: UpdateRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        client_request.ClientRequest.finishClientRequestBuffer(fbb, update.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends a get request to the host through websocket
     * @param get - The `GetRequest` object
     */
    async get(get: GetRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        client_request.ClientRequest.finishClientRequestBuffer(fbb, get.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends a subscribe request to the host through websocket
     * @param subscribe - The `SubscribeRequest` object
     */
    async subscribe(subscribe: SubscribeRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        client_request.ClientRequest.finishClientRequestBuffer(fbb, subscribe.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends an disconnect notification to the host through websocket
     * @param disconnect - The `DisconnectRequest` object
     */
    async disconnect(disconnect: DisconnectRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        client_request.ClientRequest.finishClientRequestBuffer(fbb, disconnect.pack(fbb));
        this.ws.send(fbb.asUint8Array());
        this.ws.close();
    }
}

// host replies:

/**
 * The response for a contract put operation
 * @public
 */
export type PutResponse = host_response.PutResponseT;

/**
 * The response for a contract get operation
 * @public
 */
export type GetResponse = host_response.GetResponseT;

/**
 * The response for a contract update operation
 * @public
 */
export type UpdateResponse = host_response.UpdateResponseT;

/**
 * The response for a contract update notification
 * @public
 */
export type UpdateNotification = host_response.UpdateNotificationT;

/**
 * Host reponse error type
 * @public
 */
export type HostError = {
    cause: string;
};
