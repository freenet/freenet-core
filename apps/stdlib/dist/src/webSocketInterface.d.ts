export type ContractInstanceId = Uint8Array;
export declare class Key {
    private instance;
    private code;
    constructor(instance: ContractInstanceId, code?: Uint8Array);
    static fromInstanceId(spec: string): Key;
    bytes(): ContractInstanceId;
    codePart(): Uint8Array | null;
    encode(): string;
}
export type ContractV1 = {
    key: Key;
    data: Uint8Array;
    parameters: Uint8Array;
    version: String;
};
export type State = Uint8Array;
export type StateSummary = Uint8Array;
export type StateDelta = Uint8Array;
export type UpdateData = {
    state: State;
} | {
    delta: StateDelta;
} | {
    state: State;
    delta: StateDelta;
} | {
    relatedTo: ContractInstanceId;
    state: State;
} | {
    relatedTo: ContractInstanceId;
    delta: StateDelta;
} | {
    relatedTo: ContractInstanceId;
    state: State;
    delta: StateDelta;
};
export type RelatedContracts = Map<ContractInstanceId, State | null>;
export type PutRequest = {
    container: ContractContainer;
    state: State;
    relatedContracts: RelatedContracts;
};
export type UpdateRequest = {
    key: Key;
    data: UpdateData;
};
export type GetRequest = {
    key: Key;
    fetchContract: boolean;
};
export type SubscribeRequest = {
    key: Key;
};
export type DisconnectRequest = {
    cause?: string;
};
export interface ResponseHandler {
    onPut: (response: PutResponse) => void;
    onGet: (response: GetResponse) => void;
    onUpdate: (response: UpdateResponse) => void;
    onUpdateNotification: (response: UpdateNotification) => void;
    onErr: (response: HostError) => void;
    onOpen: () => void;
}
export declare class LocutusWsApi {
    private ws;
    private encoder;
    private reponseHandler;
    constructor(url: URL, handler: ResponseHandler);
    private handleResponse;
    put(put: PutRequest): Promise<void>;
    update(update: UpdateRequest): Promise<void>;
    get(get: GetRequest): Promise<void>;
    subscribe(subscribe: SubscribeRequest): Promise<void>;
    disconnect(disconnect: DisconnectRequest): Promise<void>;
}
export type Ok = PutResponse | UpdateResponse | GetResponse | UpdateNotification;
export type HostError = {
    cause: string;
};
export interface PutResponse {
    readonly kind: "put";
    key: Key;
}
export interface UpdateResponse {
    readonly kind: "update";
    key: Key;
    summary: StateSummary;
}
export interface GetResponse {
    readonly kind: "get";
    contract?: ContractV1;
    state: State;
}
export interface UpdateNotification {
    readonly kind: "updateNotification";
    key: Key;
    update: UpdateData;
}
export declare class HostResponse {
    private result;
    constructor(bytes: Uint8Array);
    isOk(): boolean;
    unwrapOk(): Ok;
    isErr(): boolean;
    unwrapErr(): HostError;
    isPut(): boolean;
    unwrapPut(): PutResponse;
    isUpdate(): boolean;
    unwrapUpdate(): UpdateResponse;
    isGet(): boolean;
    unwrapGet(): GetResponse;
    isUpdateNotification(): boolean;
    unwrapUpdateNotification(): UpdateNotification;
    private isOfType;
    private static assertKey;
    private static assertBytes;
    private static getUpdateData;
}
type WasmContract = ContractV1;
type ContractContainer = WasmContract;
export {};
