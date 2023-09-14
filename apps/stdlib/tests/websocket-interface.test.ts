import {Server} from 'mock-socket';
import {
    ContractResponseType,
} from "../src/host-response";
import {
    DelegateResponse,
    GetResponse,
    HostError,
    LocutusWsApi,
    PutResponse,
    ResponseHandler,
    UpdateNotification,
    UpdateResponse
} from "../src";

const TEST_ENCODED_KEY = "6kVs66bKaQAC6ohr8b43SvJ95r36tc2hnG7HezmaJHF9";
const WS_URL = "ws://localhost:1234/contract/command/";

/**
 * Convert Uint8Array to ArrayBuffer.
 * @param array : Uint8Array
 */
function toBuffer(array: Uint8Array): ArrayBuffer {
    const buffer = new ArrayBuffer(array.length);
    const view = new Uint8Array(buffer);

    for (let i = 0; i < array.length; i++) {
        view[i] = array[i];
    }

    return buffer;
}

describe("Locutus Websocket API - Result Deserialization", () => {
    let server: Server;

    beforeAll(async () => {
        server = new Server(WS_URL);
    });

    afterAll(() => {
        server.clients().forEach(client => {
            client.close();
        });
        server.close();
    });

    test("should correctly deserialize Contract Put Response", async () => {
        const PUT_OP = new Uint8Array([4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1, 8, 0,
            12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 2, 222, 255, 255, 255, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0,
            4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32,
            0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75, 210, 62, 7,
            6, 34, 75, 26, 229, 230, 107, 167, 17, 108]);

        let handledResponse: ContractResponseType = ContractResponseType.NONE;
        const testHandler: ResponseHandler = {
            onContractPut: (response: PutResponse): void => {
                console.log("Received Put Response");
                expect(response.key.encode()).toEqual(TEST_ENCODED_KEY);
                handledResponse = ContractResponseType.PutResponse;
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (_response: UpdateResponse): void => {
            },
            onContractUpdateNotification: (_response: UpdateNotification): void => {
            },
            onDelegateResponse: (_response: DelegateResponse) => {
            },
            onErr: (_err: HostError): void => {
            },
            onOpen: () => {
            }
        };
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler);
        server.clients().forEach(client => {
            client.send(toBuffer(PUT_OP));
        });
        expect(handledResponse).toEqual(ContractResponseType.PutResponse);
    });

    test("should correctly deserialize Contract Get Response", async () => {
        const GET_OP = new Uint8Array([12, 0, 0, 0, 8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0,
            0, 0, 0, 0, 1, 12, 0, 0, 0, 8, 0, 14, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 1, 16, 0, 0, 0, 0, 0, 10, 0, 12,
            0, 4, 0, 0, 0, 8, 0, 10, 0, 0, 0, 16, 1, 0, 0, 4, 0, 0, 0, 250, 0, 0, 0, 123, 10, 9, 34, 109, 101, 115, 115,
            97, 103, 101, 115, 34, 58, 32, 91, 10, 9, 9, 123, 10, 9, 9, 9, 34, 97, 117, 116, 104, 111, 114, 34, 58, 32,
            34, 73, 68, 71, 34, 44, 10, 9, 9, 9, 34, 100, 97, 116, 101, 34, 58, 32, 34, 50, 48, 50, 50, 45, 48, 53, 45,
            49, 48, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 10, 9, 9, 9, 34, 116, 105, 116, 108, 101, 34, 58,
            32, 34, 76, 111, 114, 101, 32, 105, 112, 115, 117, 109, 34, 44, 10, 9, 9, 9, 34, 99, 111, 110, 116, 101,
            110, 116, 34, 58, 32, 34, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114,
            32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32,
            97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111,
            32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105,
            100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 111, 108, 111, 114,
            101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 34, 10, 9, 9, 125, 10, 9, 93, 10, 125,
            0, 0, 8, 0, 12, 0, 4, 0, 8, 0, 8, 0, 0, 0, 52, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 175, 19, 73, 185, 245, 249,
            161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203, 37, 201, 173, 193, 18, 183, 204, 154, 147, 202, 228,
            31, 50, 98, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177,
            207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108]);

        let handledResponse: ContractResponseType = ContractResponseType.NONE;
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (response: GetResponse): void => {
                console.log("Received Get Response");
                expect(response.key.encode()).toEqual(TEST_ENCODED_KEY);
                handledResponse = ContractResponseType.GetResponse;
            },
            onContractUpdate: (_response: UpdateResponse): void => {
            },
            onContractUpdateNotification: (_response: UpdateNotification): void => {
            },
            onDelegateResponse: (_response: DelegateResponse) => {
            },
            onErr: (_err: HostError): void => {
            },
            onOpen: () => {
            },
        };
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler);

        server.clients().forEach(client => {
            client.send(toBuffer(GET_OP));
        });

        expect(handledResponse).toEqual(ContractResponseType.GetResponse);
    });

    // Add test fot UPDATE_NOTIFICATION_OP.
    test("should correctly deserialize Contract Update Notification", async () => {
        const UPDATE_NOTIFICATION_OP = new Uint8Array([4, 0, 0, 0, 220, 255, 255, 255, 8, 0, 0, 0,
            0, 0, 0, 1, 232, 255, 255, 255, 8, 0, 0, 0, 0, 0, 0, 3, 204, 255, 255, 255, 16, 0, 0, 0, 52, 0, 0, 0, 8, 0,
            12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 2, 210, 255, 255, 255, 4, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3,
            4, 5, 6, 7, 8, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0,
            4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173, 90,
            234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108]);
        let handledResponse: ContractResponseType = ContractResponseType.NONE;
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (_response: UpdateResponse): void => {
            },
            onContractUpdateNotification: (response: UpdateNotification): void => {
                console.log("Received Update Notification");
                expect(response.key.encode()).toEqual(TEST_ENCODED_KEY);
                handledResponse = ContractResponseType.UpdateNotification;
            },
            onDelegateResponse: (_response: DelegateResponse) => {
            },
            onErr: (_err: HostError): void => {
            },
            onOpen: () => {
            },
        };
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler);
        server.clients().forEach(client => {
            client.send(toBuffer(UPDATE_NOTIFICATION_OP));
        });
        expect(handledResponse).toEqual(ContractResponseType.UpdateNotification);
    });

    // Test for UPDATE_OP.
    test("should correctly deserialize Contract Update", async () => {
        const UPDATE_OP = new Uint8Array([4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1,
            8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 240, 255, 255, 255, 8, 0, 0, 0, 16, 0, 0, 0,
            0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4,
            0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173, 90, 234,
            2, 250, 253, 75, 210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108]);
        let handledResponse: ContractResponseType = ContractResponseType.NONE;
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (response: UpdateResponse): void => {
                console.log("Received Update");
                expect(response.key.encode()).toEqual(TEST_ENCODED_KEY);
                handledResponse = ContractResponseType.UpdateResponse;
            },
            onContractUpdateNotification: (_response: UpdateNotification): void => {
            },
            onDelegateResponse: (_response: DelegateResponse) => {
            },
            onErr: (_err: HostError): void => {
            },
            onOpen: () => {
            },
        };
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler);
        server.clients().forEach(client => {
            client.send(toBuffer(UPDATE_OP));
        });
        expect(handledResponse).toEqual(ContractResponseType.UpdateResponse);
    });
});
