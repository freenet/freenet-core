import { decode, Encoder } from "@msgpack/msgpack"

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

    async put(put: PutRequest) {
        let encoded = this.encoder.encode(put);
        this.ws.send(encoded);
    }

    async update(get: UpdateRequest) {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async get(get: GetRequest) {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async subscribe(get: SubscribeRequest) {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
    }

    async disconnect(get: DisconnectRequest) {
        let encoded = this.encoder.encode(get);
        this.ws.send(encoded);
        this.ws.close();
    }
}

// ws server response types:
type Ok = PutResponse | UpdateResponse;
type Error = {
    cause: string
}
type PutResponse = {}
type UpdateResponse = {}

type HostResponse = Ok | Error;

// base interface types:

export type Key = Uint8Array;

export type Contract = {
    key: Key,
    data: Uint8Array,
    parameters: Uint8Array
}

export type State = Uint8Array;

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
