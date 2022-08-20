import { decode, Encoder } from "@msgpack/msgpack"
import base58 from "bs58"

const MAX_U8: number = 255;
const MIN_U8: number = 0;

// base interface types:

/**
 * The key representing the tuple of a contract code and a set of parameters.
 */
export class Key {
    private spec: Uint8Array;
    private contract: Uint8Array | null

    constructor(spec: Uint8Array, contract?: Uint8Array) {
        if (spec.length != 32 || typeof contract != "undefined" && contract.length != 32) {
            throw TypeError("invalid array lenth (expected 32 bytes): " + spec.length);
        }
        this.spec = spec;
        if (typeof contract == "undefined") {
            this.contract = null;
        } else {
            this.contract = contract;
        }
    }

    static fromSpec(spec: string): Key {
        let encoded = base58.decode(spec);
        return new Key(encoded);
    }

    /**
     * @returns {Uint8Array} Hash of the full key specification (contract code + parameter).
     */
    bytes(): Uint8Array {
        return this.spec;
    }

    /**
     * @returns {Uint8Array | null} Hash of the contract code part of the full specification.
     */
    contractPart(): Uint8Array | null {
        return this.contract;
    }

    /**
     * Generates the full key specification (contract code + parameter) encoded as base58 string.
     * 
     * @returns {string} The encoded string representation.
     */
    encode(): string {
        return base58.encode(this.spec)
    }
}

export type Contract = {
    key: Key,
    data: Uint8Array,
    parameters: Uint8Array
}

export type State = Uint8Array;
export type StateSummary = Uint8Array;
export type StateDelta = Uint8Array;

// ops:

export type PutRequest = {
    contract: Contract,
    state: State
}

export type UpdateRequest = {
    key: Key,
    delta: Uint8Array
}

export type GetRequest = {
    key: Key,
    fetch_contract: boolean
}

export type SubscribeRequest = {
    key: Key
}

export type DisconnectRequest = {
    cause?: string
}

// API

export interface ResponseHandler {
    onPut: (response: PutResponse) => void,
    onGet: (response: GetResponse) => void,
    onUpdate: (response: UpdateResponse) => void,
    onUpdateNotification: (response: UpdateNotification) => void,
    onErr: (response: HostError) => void,
    onOpen: () => void,
}

export class LocutusWsApi {
    public ws: WebSocket
    private encoder: Encoder
    private reponseHandler: ResponseHandler

    constructor(url: URL, handler: ResponseHandler) {
        this.ws = new WebSocket(url);
        this.ws.binaryType = 'arraybuffer';
        this.encoder = new Encoder();
        this.reponseHandler = handler;
        this.ws.onmessage = (ev) => {
            this.handleResponse(ev);
        };
        this.ws.addEventListener('open', (_) => {
            handler.onOpen();
        });
    }

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
                    this.reponseHandler.onUpdateNotification(response.unwrapUpdateNotification());
            }
        } else {
            this.reponseHandler.onErr(response.unwrapErr())
        }
    }

    async put(put: PutRequest): Promise<void> {
        let encoded = this.encoder.encode(put);
        this.ws.send(encoded);
    }

    async update(update: UpdateRequest): Promise<void> {
        let encoded = this.encoder.encode(update);
        this.ws.send(encoded);
    }

    async get(get: GetRequest): Promise<void> {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async subscribe(subscribe: SubscribeRequest): Promise<void> {
        let encoded = this.encoder.encode(subscribe);
        this.ws.send(encoded);
    }

    async disconnect(disconnect: DisconnectRequest): Promise<void> {
        let encoded = this.encoder.encode(disconnect);
        this.ws.send(encoded);
        this.ws.close();
    }
}

// host replies:

export type Ok = PutResponse | UpdateResponse | GetResponse | UpdateNotification;
export type HostError = {
    cause: string
}

export interface PutResponse {
    readonly kind: "put"
    key: Key
}

export interface UpdateResponse {
    readonly kind: "update"
    key: Key
    summary: State
}

export interface GetResponse {
    readonly kind: "get"
    contract?: Contract,
    state: State,
}

export interface UpdateNotification {
    readonly kind: "updateNotification"
    key: Key
    update: StateDelta
}

function assert(condition: boolean, msg?: string) {
    if (!condition)
        throw new TypeError(msg)
}

export class HostResponse {

    private result: Ok | HostError

    constructor(bytes: Uint8Array) {
        let decoded = decode(bytes) as object;
        if ("Ok" in decoded) {
            let ok = decoded as { "Ok": any };
            if ("PutResponse" in ok.Ok) {
                ok.Ok as { "PutResponse": any };
                assert(Array.isArray(ok.Ok.PutResponse));
                let key = HostResponse.assertKey(ok.Ok.PutResponse[0][0]);
                this.result = { kind: "put", key };
                return;
            } else if ("UpdateResponse" in ok.Ok) {
                ok.Ok as { "UpdateResponse": any };
                assert(Array.isArray(ok.Ok.UpdateResponse));
                assert(ok.Ok.UpdateResponse.length == 2);
                let key = HostResponse.assertKey(ok.Ok.UpdateResponse[0][0]);
                let summary = HostResponse.assertBytes(ok.Ok.UpdateResponse[1]);
                this.result = { kind: "update", key, summary };
                return;
            } else if ("GetResponse" in ok.Ok) {
                ok.Ok as { "GetResponse": any };
                assert(Array.isArray(ok.Ok.GetResponse));
                assert(ok.Ok.GetResponse.length == 2);
                let contract;
                if (ok.Ok.GetResponse[0] !== null) {
                    contract = {
                        data: new Uint8Array(ok.Ok.GetResponse[0][0][1]),
                        parameters: new Uint8Array(ok.Ok.GetResponse[0][1]),
                        key: new Key(ok.Ok.GetResponse[0][2][0])
                    }
                }
                else {
                    contract = null;
                }
                let get = {
                    kind: "get",
                    contract,
                    state: ok.Ok.GetResponse[1]
                };
                this.result = get as GetResponse;
                return;
            } else if ("UpdateNotification" in ok.Ok) {
                ok.Ok as { "UpdateNotification": any };
                assert(Array.isArray(ok.Ok.UpdateNotification));
                assert(ok.Ok.UpdateNotification.length == 2);
                let key = HostResponse.assertKey(ok.Ok.UpdateNotification[0][0]);
                let update = HostResponse.assertBytes(ok.Ok.UpdateNotification[1]);
                this.result = { kind: "updateNotification", key, update } as UpdateNotification;
                return;
            }
        } else if ("Err" in decoded) {
            let err = decoded as { "Err": Array<any> };
            if ("RequestError" in err.Err[0]) {
                if ("Update" in err.Err[0].RequestError) {
                    let updateErr = err.Err[0].RequestError.Update as Array<Array<Uint8Array>>;
                    let contractKey = new Key(updateErr[0][0]);
                    this.result = { cause: "Update error for contract " + contractKey.encode() };
                    return;
                }
            }
        }
        throw new TypeError("bytes are not a valid HostResponse");
    }

    isOk(): boolean {
        if ("kind" in this.result)
            return true
        else
            return false
    }

    unwrapOk(): Ok {
        if ("kind" in this.result) {
            return this.result
        }
        else
            throw new TypeError
    }

    isErr(): boolean {
        if (this.result instanceof Error)
            return true
        else
            return false;
    }

    unwrapErr(): HostError {
        if (this.result instanceof Error)
            return this.result as HostError
        else
            throw new TypeError
    }

    isPut(): boolean {
        return this.isOfType("put")
    }

    unwrapPut(): PutResponse {
        if (this.isOfType("put"))
            return this.result as PutResponse
        else
            throw new TypeError
    }

    isUpdate(): boolean {
        return this.isOfType("update")
    }

    unwrapUpdate(): UpdateResponse {
        if (this.isOfType("update"))
            return this.result as UpdateResponse
        else
            throw new TypeError
    }

    isGet(): boolean {
        return this.isOfType("get")
    }

    unwrapGet(): GetResponse {
        if (this.isOfType("get"))
            return this.result as GetResponse
        else
            throw new TypeError
    }

    isUpdateNotification(): boolean {
        return this.isOfType("updateNotification")
    }

    unwrapUpdateNotification(): UpdateNotification {
        if (this.isOfType("updateNotification"))
            return this.result as UpdateNotification
        else
            throw new TypeError
    }

    private isOfType(ty: string): boolean {
        return "kind" in this.result && this.result.kind === ty
    }

    private static assertKey(key: any): Key {
        let bytes = HostResponse.assertBytes(key);
        assert(bytes.length === 32, "expected exactly 32 bytes");
        return new Key(bytes as Uint8Array);
    }

    private static assertBytes(state: any): Uint8Array {
        assert(Array.isArray(state));
        assert(state.every((value: any) => {
            if (typeof value === 'number' && value >= MIN_U8 && value <= MAX_U8)
                return true;
            else
                return false;
        }), "expected an array of bytes");
        return state as Uint8Array;
    }
}
