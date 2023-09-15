import {Server} from 'mock-socket';
import {ContractResponseType,} from "../src/host-response";
import {
    ContractContainer,
    ContractKey,
    DelegateResponse, DeltaUpdate,
    GetRequest,
    GetResponse,
    HostError,
    LocutusWsApi,
    PutRequest,
    PutResponse,
    ResponseHandler,
    UpdateData,
    UpdateNotification, UpdateRequest,
    UpdateResponse,
    WasmContractV1
} from "../src";
import {ContractType} from "../src/common/contract-type";
import {ContractCodeT} from "../src/common/contract-code";
import {RelatedContractsT} from "../src/client-request/related-contracts";
import {UpdateDataType} from "../src/common/update-data-type";

const TEST_ENCODED_KEY = "6kVs66bKaQAC6ohr8b43SvJ95r36tc2hnG7HezmaJHF9";
const AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
const WS_URL = "ws://localhost:1234/contract/command/";

describe("Locutus Websocket API - Result Deserialization", () => {
    let server: Server;

    beforeAll(() => {
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
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        server.clients().forEach(client => {
            client.send(PUT_OP.buffer);
        });
        expect(handledResponse).toEqual(ContractResponseType.PutResponse);
    });

    test("should correctly deserialize Contract Get Response", () => {
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
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);

        server.clients().forEach(client => {
            client.send(GET_OP.buffer);
        });

        expect(handledResponse).toEqual(ContractResponseType.GetResponse);
    });

    test("should correctly deserialize Contract Update Notification", () => {
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
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        server.clients().forEach(client => {
            client.send(UPDATE_NOTIFICATION_OP.buffer);
        });
        expect(handledResponse).toEqual(ContractResponseType.UpdateNotification);
    });

    test("should correctly deserialize Contract Update", () => {
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
        const _api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        server.clients().forEach(client => {
            client.send(UPDATE_OP.buffer);
        });
        expect(handledResponse).toEqual(ContractResponseType.UpdateResponse);
    });

    test("should correctly generate a ClientRequest for contract put operation", async () => {
        // Define the expected Uint8Array request
        let EXPECTED_PUT_REQ = new Uint8Array([
            4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1, 198, 255, 255, 255, 12, 0, 0, 0, 20, 0, 0, 0, 36, 0,
            0, 0, 170, 255, 255, 255, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            8, 0, 10, 0, 9, 0, 4, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 1, 10, 0, 16, 0, 12, 0, 8, 0, 4,
            0, 10, 0, 0, 0, 12, 0, 0, 0, 76, 0, 0, 0, 92, 0, 0, 0, 176, 255, 255, 255, 8, 0, 0, 0,
            16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0,
            85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75,
            210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6,
            7, 8, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 1, 2,
            3, 4, 5, 6, 7, 8, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8
        ]);

        // Build the contract put operation object
        let data = Array.from(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
        let codeHash = Array.from(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
        let contractCode = new ContractCodeT(data, codeHash);
        let params = Array.from(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
        let key = ContractKey.fromInstanceId(TEST_ENCODED_KEY);
        let contract = new WasmContractV1(contractCode, params, key);

        let container = new ContractContainer(ContractType.WasmContractV1, contract);

        let state = Array.from(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
        let relatedContracts = new RelatedContractsT([]);
        let putRequest = new PutRequest(container, state, relatedContracts);

        // Create a stubbed response handler
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (response: UpdateResponse): void => {
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

        // Flag to indicate if the expected PUT request was received
        let receivedExpectedPutReq = false;

        // Mock server behavior
        server.on('connection', socket => {
            socket.on('message', data => {
                try {
                    expect(data).toEqual(EXPECTED_PUT_REQ);
                    receivedExpectedPutReq = true;
                } catch (error) {
                    console.log("Received unexpected message");
                }
            });
        });

        // Instantiate API and perform operations
        const api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await api.put(putRequest);
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Verify that the expected PUT request was received
        expect(receivedExpectedPutReq).toEqual(true);
    });

    test("should correctly generate a ClientRequest for contract get operation", async () => {
        // Define the expected Uint8Array request
        let EXPECTED_PUT_REQ = new Uint8Array([
            4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 3, 222, 255, 255, 255, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4,
            0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0,
            4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173,
            90, 234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108
        ]);

        // Build the contract get operation object
        let key = ContractKey.fromInstanceId(TEST_ENCODED_KEY);
        let getRequest = new GetRequest(key, false);

        // Create a stubbed response handler
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (response: UpdateResponse): void => {
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

        // Flag to indicate if the expected GET request was received
        let receivedExpectedGetReq = false;

        // Mock server behavior
        server.on('connection', socket => {
            socket.on('message', data => {
                try {
                    expect(data).toEqual(EXPECTED_PUT_REQ);
                    receivedExpectedGetReq = true;
                } catch (error) {
                    console.log("Received unexpected message");
                }
            });
        });

        // Instantiate API and perform operations
        const api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await api.get(getRequest);
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Verify that the expected GET request was received
        expect(receivedExpectedGetReq).toEqual(true);
    });

    test("should correctly generate a ClientRequest for contract update operation", async () => {
        // Define the expected Uint8Array request
        let EXPECTED_UPDATE_REQ = new Uint8Array([
            4, 0, 0, 0, 220, 255, 255, 255, 8, 0, 0, 0, 0, 0, 0, 1, 232, 255, 255, 255, 8, 0, 0, 0,
            0, 0, 0, 2, 204, 255, 255, 255, 16, 0, 0, 0, 52, 0, 0, 0, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 2, 210, 255, 255, 255, 4, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3,
            4, 5, 6, 7, 8, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40,
            85, 240, 177, 207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75,
            26, 229, 230, 107, 167, 17, 108
        ]);

        // Build the contract update operation object
        let key = ContractKey.fromInstanceId(TEST_ENCODED_KEY);

        let delta = Array.from(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
        let deltaUpdate = new DeltaUpdate(delta);
        let updateData = new UpdateData(UpdateDataType.DeltaUpdate, deltaUpdate);

        let updateRequest = new UpdateRequest(key, updateData);

        // Create a stubbed response handler
        const testHandler: ResponseHandler = {
            onContractPut: (_response: PutResponse): void => {
            },
            onContractGet: (_response: GetResponse): void => {
            },
            onContractUpdate: (response: UpdateResponse): void => {
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

        // Flag to indicate if the expected UPDATE request was received
        let receivedExpectedUpdateReq = false;

        // Mock server behavior
        server.on('connection', socket => {
            socket.on('message', data => {
                try {
                    expect(data).toEqual(EXPECTED_UPDATE_REQ);
                    receivedExpectedUpdateReq = true;
                } catch (error) {
                    console.log("Received unexpected message");
                }
            });
        });

        // Instantiate API and perform operations
        const api = new LocutusWsApi(new URL(WS_URL), testHandler, AUTH_TOKEN);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await api.update(updateRequest);
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Verify that the expected UPDATE request was received
        expect(receivedExpectedUpdateReq).toEqual(true);
    });
});
