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
        this.spec = spec;
        if (typeof contract == "undefined") {
            this.contract = null;
        } else {
            this.contract = contract;
        }
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
    contract_part(): Uint8Array | null {
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

export class LocutusWsApi {
    private ws: WebSocket
    private encoder: Encoder

    constructor(url: URL, _handler: (response: HostResponse) => void) {
        this.ws = new WebSocket(url);
        this.encoder = new Encoder();
        this.ws.onmessage = (ev) => {
            let decoded = decode(ev.data);
            console.log(decoded)
            // handler(decoded)
        };
    }

    async put(put: PutRequest): Promise<void> {
        let encoded = this.encoder.encode(put);
        this.ws.send(encoded);
    }

    async update(get: UpdateRequest): Promise<void> {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async get(get: GetRequest): Promise<void> {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async subscribe(get: SubscribeRequest): Promise<void> {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async disconnect(get: DisconnectRequest): Promise<void> {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
        this.ws.close();
    }
}

// host replies:

type Ok = PutResponse | UpdateResponse | GetResponse | UpdateNotification;
type Error = {
    cause: string
}

interface PutResponse {
    readonly kind: "put"
    key: Key
}

interface UpdateResponse {
    readonly kind: "update"
    key: Key
    summary: State
}

interface GetResponse {
    readonly kind: "get"
    contract?: Contract,
    state: State,
}

interface UpdateNotification {
    readonly kind: "update_notification"
    key: Key
    update: StateDelta
}

function assert(condition: boolean, msg?: string) {
    if (!condition)
        throw new TypeError(msg)
}

export class HostResponse {

    private result: Ok | Error

    constructor(bytes: Uint8Array) {
        let decoded = decode(bytes) as object;
        if ("Ok" in decoded) {
            let ok = decoded as { "Ok": any };
            if ("PutResponse" in ok.Ok) {
                ok.Ok as { "PutResponse": any };
                assert(Array.isArray(ok.Ok.PutResponse));
                let key = HostResponse.assert_key(ok.Ok.PutResponse[0][0]);
                this.result = { kind: "put", key };
                return;
            } else if ("UpdateResponse" in ok.Ok) {
                ok.Ok as { "UpdateResponse": any };
                assert(Array.isArray(ok.Ok.UpdateResponse));
                assert(ok.Ok.UpdateResponse.length == 2);
                let key = HostResponse.assert_key(ok.Ok.UpdateResponse[0][0]);
                let summary = HostResponse.assert_bytes(ok.Ok.UpdateResponse[1]);
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
                let key = HostResponse.assert_key(ok.Ok.UpdateNotification[0][0]);
                let update = HostResponse.assert_bytes(ok.Ok.UpdateNotification[1]);
                this.result = { kind: "update_notification", key, update } as UpdateNotification;
                return;
            }
        }
        throw new TypeError("bytes are not a valid HostResponse");
    }

    is_ok(): boolean {
        if ("kind" in this.result)
            return true
        else
            return false
    }

    unwrap_ok(): Ok {
        if ("kind" in this.result) {
            return this.result
        }
        else
            throw new TypeError
    }

    is_err(): boolean {
        if (this.result instanceof Error)
            return true
        else
            return false;
    }

    unwrap_err(): Error {
        if (this.result instanceof Error)
            return this.result as Error
        else
            throw new TypeError
    }

    is_put(): boolean {
        return this.is_of_type("put")
    }

    unwrap_put(): PutResponse {
        if (this.is_of_type("put"))
            return this.result as PutResponse
        else
            throw new TypeError
    }

    is_update(): boolean {
        return this.is_of_type("update")
    }

    unwrap_update(): UpdateResponse {
        if (this.is_of_type("update"))
            return this.result as UpdateResponse
        else
            throw new TypeError
    }

    is_get(): boolean {
        return this.is_of_type("get")
    }

    unwrap_get(): GetResponse {
        if (this.is_of_type("get"))
            return this.result as GetResponse
        else
            throw new TypeError
    }

    is_update_notification(): boolean {
        return this.is_of_type("update_notification")
    }

    unwrap_update_notification(): UpdateNotification {
        if (this.is_of_type("update_notification"))
            return this.result as UpdateNotification
        else
            throw new TypeError
    }

    private is_of_type(ty: string): boolean {
        return "kind" in this.result && this.result.kind === ty
    }

    private static assert_key(key: any): Key {
        let bytes = HostResponse.assert_bytes(key);
        assert(bytes.length === 32, "expected exactly 32 bytes");
        return new Key(bytes as Uint8Array);
    }

    private static assert_bytes(state: any): Uint8Array {
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
