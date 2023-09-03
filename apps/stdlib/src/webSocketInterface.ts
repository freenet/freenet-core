import flatbuffers from 'flatbuffers';
import base58 from "bs58";

import {ContractContainerT} from "./common/contract-container";
import {ContractInstanceIdT} from "./common/contract-instance-id";
import {DeltaUpdateT} from "./common/delta-update";
import {RelatedDeltaUpdateT} from "./common/related-delta-update";
import {RelatedStateAndDeltaUpdateT} from "./common/related-state-and-delta-update";
import {RelatedStateUpdateT} from "./common/related-state-update";
import {StateAndDeltaUpdateT} from "./common/state-and-delta-update";
import {StateUpdateT} from "./common/state-update";
import {
    ClientRequest,
    DelegateContainerT,
    DisconnectT,
    GetT,
    PutT,
    RelatedContractsT,
    SubscribeT,
    UpdateT
} from "./client-request";
import {UpdateDataT} from "./common/update-data";
import {UpdateDataType} from "./common/update-data-type";
import {ContractKeyT} from "./common/contract-key";
import {PutResponseT} from "./host-response/put-response";
import {GetResponseT} from "./host-response/get-response";
import {UpdateResponseT} from "./host-response/update-response";
import {UpdateNotificationT} from "./host-response/update-notification";
import {HostResponse, HostResponseT} from "./host-response/host-response";
import {ContractResponseT, ContractResponseType, HostResponseType} from "./host-response";

/**
 * The id of a live instance of a contract. This is effectively the tuple
 * of the hash of the hash of the contract code and a set of parameters used to run
 * the contract.
 * @public
 */
export type ContractInstanceId = Uint8Array;

/**
 * Wrapper that allows contract versioning. This enum maintains the types of contracts that are allowed
 * and their corresponding version.
 * @public
 */
export type ContractContainer = ContractContainerT;

/**
 * Wrapper that allows delegate versioning. This enum maintains the types of delegates that are allowed
 * and their corresponding version.
 * @public
 */
export type DelegateContainer = DelegateContainerT;


/**
 * Representation of the client put request operation
 * @public
 */
export class PutRequest extends PutT {
    constructor(container: ContractContainerT | null = null,
                wrappedState: (number)[] = [],
                relatedContracts: RelatedContractsT | null = null) {
        super(container, wrappedState, relatedContracts);
    }
}

/**
 * Representation of the client update request operation
 * @public
 */
export class UpdateRequest extends UpdateT {
    constructor(key: Key | null = null, update: UpdateDataT | null = null) {
        const contract_key = key?.get_contract_key();
        super(contract_key, update);
    }
}

/**
 * Representation of the client get request operation
 * @public
 */
export class GetRequest extends GetT {
    constructor(key: Key | null = null, fetchContract: boolean = false) {
        const contract_key = key?.get_contract_key();
        super(contract_key, fetchContract);
    }
}

/**
 * Representation of the client subscribe request operation
 * @public
 */
export class SubscribeRequest extends SubscribeT {
    constructor(key: Key | null = null, summary: (number)[] = []) {
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

export class UpdateData extends UpdateDataT {
    constructor(updateDataType: UpdateDataType = UpdateDataType.NONE,
                updateData: DeltaUpdateT | RelatedDeltaUpdateT | RelatedStateAndDeltaUpdateT | RelatedStateUpdateT
                    | StateAndDeltaUpdateT | StateUpdateT | null = null) {
        super(updateDataType, updateData);
    }
}

/**
 * Representation of the state update data
 * @public
 */
export class StateUpdate extends StateUpdateT {
    constructor(state: (number)[] = []) {
        super(state);
    }
}

/**
 * Representation of the delta update data
 * @public
 */
export class DeltaUpdate extends DeltaUpdateT {
    constructor(delta: (number)[] = []) {
        super(delta);
    }
}

/**
 * Representation of the state and delta update data
 * @public
 */
export class StateAndDeltaUpdate extends StateAndDeltaUpdateT {
    constructor(state: (number)[] = [], delta: (number)[] = []) {
        super(state, delta);
    }
}

/**
 * Representation of the related state update data
 * @public
 */
export class RelatedStateUpdate extends RelatedStateUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, state: (number)[] = []) {
        super(relatedTo, state);
    }
}

/**
 * Representation of the related delta update data
 * @public
 */
export class RelatedDeltaUpdate extends RelatedDeltaUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, delta: (number)[] = []) {
        super(relatedTo, delta);
    }
}

/**
 * Representation of the related state and delta update data
 * @public
 */
export class RelatedStateAndDeltaUpdate extends RelatedStateAndDeltaUpdateT {
    constructor(relatedTo: ContractInstanceIdT | null = null, state: (number)[] = [], delta: (number)[] = []) {
        super(relatedTo, state, delta);
    }
}


export class Key extends ContractKeyT {
    constructor(instance: ContractInstanceId, code?: Uint8Array) {
        if (instance.length !== 32 || (code && code.length !== 32)) {
            throw new TypeError('Invalid array length, expected 32 bytes');
        }

        let contract_instance_id = new ContractInstanceIdT(Array.from(instance))
        let contract_code: (number)[] = [];
        if (code) {
            contract_code = Array.from(code);
        }
        super(contract_instance_id, contract_code);
    }

    static fromInstanceId(spec: string): Key {
        const decoded = base58.decode(spec);
        return new Key(decoded);
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

    get_contract_key(): Key {
        return this;
    }
}

// host replies:

/**
 * The response for a contract put operation
 * @public
 */
export type PutResponse = PutResponseT;

/**
 * The response for a contract get operation
 * @public
 */
export type GetResponse = GetResponseT;

/**
 * The response for a contract update operation
 * @public
 */
export type UpdateResponse = UpdateResponseT;

/**
 * The response for a contract update notification
 * @public
 */
export type UpdateNotification = UpdateNotificationT;

/**
 * Host reponse error type
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
        let response: HostResponseT;
        try {
            let data = new flatbuffers.ByteBuffer(ev.data);
            response = new HostResponseT();
            HostResponse.getRootAsHostResponse(data).unpackTo(response);
        } catch (err) {
            console.log(`found error: ${err}`);
            return new Error(`${err}`);
        }
        switch (response.responseType) {
            case HostResponseType.ContractResponse:
                let host_resp = response.response as ContractResponseT;
                switch (host_resp.contractResponseType) {
                    case ContractResponseType.PutResponse:
                        const put_response = host_resp.contractResponse as PutResponseT;
                        this.reponseHandler.onPut(put_response);
                        break;
                    case ContractResponseType.GetResponse:
                        const get_response = host_resp.contractResponse as GetResponseT;
                        this.reponseHandler.onGet(get_response);
                        break;
                    case ContractResponseType.UpdateResponse:
                        const update_response = host_resp.contractResponse as UpdateResponseT;
                        this.reponseHandler.onUpdate(update_response);
                        break;
                    case ContractResponseType.UpdateNotification:
                        const update_notification = host_resp.contractResponse as UpdateNotificationT;
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
            case HostResponseType.DelegateResponse:
                console.log(`Delegate response handler not implemented`);
                break;
            case HostResponseType.GenerateRandData:
                console.log(`GenerateRandData response handler not implemented`);
                break;
            case HostResponseType.Ok:
                console.log(`not implemented`);
                break;
            case HostResponseType.NONE:
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
        ClientRequest.finishClientRequestBuffer(fbb, put.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends an update request to the host through websocket
     * @param update - The `UpdateRequest` object
     */
    async update(update: UpdateRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        ClientRequest.finishClientRequestBuffer(fbb, update.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends a get request to the host through websocket
     * @param get - The `GetRequest` object
     */
    async get(get: GetRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        ClientRequest.finishClientRequestBuffer(fbb, get.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends a subscribe request to the host through websocket
     * @param subscribe - The `SubscribeRequest` object
     */
    async subscribe(subscribe: SubscribeRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        ClientRequest.finishClientRequestBuffer(fbb, subscribe.pack(fbb));
        this.ws.send(fbb.asUint8Array());
    }

    /**
     * Sends an disconnect notification to the host through websocket
     * @param disconnect - The `DisconnectRequest` object
     */
    async disconnect(disconnect: DisconnectRequest): Promise<void> {
        let fbb = new flatbuffers.Builder(1024);
        ClientRequest.finishClientRequestBuffer(fbb, disconnect.pack(fbb));
        this.ws.send(fbb.asUint8Array());
        this.ws.close();
    }
}
