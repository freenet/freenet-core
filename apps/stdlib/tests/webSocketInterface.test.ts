import {HostResponse} from "../src/webSocketInterface";

describe("locutus websocket API ok result deserialization", () => {
  test("put op deserialization", () => {
    const PUT_OP_FULL_KEY = new Uint8Array([
      129, 162, 79, 107, 129, 171, 80, 117, 116, 82, 101, 115, 112, 111, 110,
      115, 101, 145, 146, 220, 0, 32, 204, 235, 204, 170, 107, 204, 172, 204,
      207, 204, 233, 4, 116, 117, 10, 77, 204, 214, 204, 180, 204, 220, 46, 204,
      240, 204, 207, 58, 204, 184, 70, 74, 121, 204, 178, 48, 204, 138, 204,
      179, 204, 171, 204, 250, 204, 145, 204, 151, 121, 204, 194, 220, 0, 32,
      105, 33, 122, 48, 121, 204, 144, 204, 128, 204, 148, 204, 225, 17, 33,
      204, 208, 66, 53, 74, 124, 31, 85, 204, 182, 72, 44, 204, 161, 204, 165,
      30, 27, 37, 13, 204, 253, 30, 204, 208, 204, 238, 204, 249,
    ]);
    let decoded0 = new HostResponse(PUT_OP_FULL_KEY);
    expect(decoded0.isPut());

    const PUT_OP_ONLY_SPEC = new Uint8Array([
      129, 162, 79, 107, 129, 171, 80, 117, 116, 82, 101, 115, 112, 111, 110,
      115, 101, 145, 146, 220, 0, 32, 204, 235, 204, 170, 107, 204, 172, 204,
      207, 204, 233, 4, 116, 117, 10, 77, 204, 214, 204, 180, 204, 220, 46, 204,
      240, 204, 207, 58, 204, 184, 70, 74, 121, 204, 178, 48, 204, 138, 204,
      179, 204, 171, 204, 250, 204, 145, 204, 151, 121, 204, 194, 192,
    ]);
    let decoded1 = new HostResponse(PUT_OP_ONLY_SPEC);
    expect(decoded1.isPut());

    expect(decoded0.unwrapPut().key).toStrictEqual(decoded1.unwrapPut().key);
  });

  test("update op deserialization", () => {
    const UPDATE_OP = new Uint8Array([
      129, 162, 79, 107, 129, 174, 85, 112, 100, 97, 116, 101, 82, 101, 115,
      112, 111, 110, 115, 101, 146, 146, 220, 0, 32, 204, 137, 204, 226, 204,
      216, 204, 143, 102, 204, 197, 204, 178, 204, 160, 204, 203, 59, 204, 130,
      34, 46, 92, 115, 108, 9, 112, 204, 179, 204, 191, 204, 232, 204, 230, 204,
      143, 204, 248, 120, 83, 15, 98, 68, 11, 114, 204, 226, 220, 0, 32, 86,
      204, 197, 204, 151, 101, 37, 61, 121, 77, 23, 94, 204, 244, 204, 182, 204,
      244, 204, 224, 204, 199, 204, 239, 106, 204, 247, 95, 127, 204, 158, 204,
      235, 204, 172, 204, 195, 204, 139, 32, 38, 204, 132, 204, 179, 204, 139,
      204, 152, 204, 167, 145, 204, 216,
    ]);
    let decoded = new HostResponse(UPDATE_OP);
    expect(decoded.isUpdate());
  });

  test("get op deserialization", () => {
    const GET_OP_WITH_CONTRACT = new Uint8Array([
      129, 162, 79, 107, 129, 171, 71, 101, 116, 82, 101, 115, 112, 111, 110,
      115, 101, 146, 147, 146, 145, 204, 135, 220, 0, 32, 204, 129, 204, 152,
      204, 176, 204, 178, 204, 226, 57, 47, 204, 208, 105, 204, 222, 1, 204,
      160, 204, 141, 46, 16, 204, 161, 84, 52, 204, 207, 85, 53, 204, 202, 101,
      92, 104, 204, 240, 204, 178, 69, 124, 204, 169, 38, 75, 145, 85, 146, 220,
      0, 32, 204, 152, 204, 243, 204, 166, 89, 30, 54, 204, 239, 89, 204, 172,
      105, 16, 204, 251, 204, 220, 204, 226, 102, 204, 161, 42, 83, 204, 142,
      204, 192, 204, 156, 94, 11, 47, 204, 136, 204, 181, 204, 189, 204, 233,
      55, 27, 204, 210, 28, 220, 0, 32, 204, 129, 204, 152, 204, 176, 204, 178,
      204, 226, 57, 47, 204, 208, 105, 204, 222, 1, 204, 160, 204, 141, 46, 16,
      204, 161, 84, 52, 204, 207, 85, 53, 204, 202, 101, 92, 104, 204, 240, 204,
      178, 69, 124, 204, 169, 38, 75, 196, 3, 18, 146, 12,
    ]);
    let decoded0 = new HostResponse(GET_OP_WITH_CONTRACT);
    expect(decoded0.isGet());
    expect(decoded0.unwrapGet().contract).toBeTruthy();

    const GET_OP_WITHOUT_CONTRACT = new Uint8Array([
      129, 162, 79, 107, 129, 171, 71, 101, 116, 82, 101, 115, 112, 111, 110,
      115, 101, 146, 192, 196, 3, 18, 146, 12,
    ]);
    let decoded1 = new HostResponse(GET_OP_WITHOUT_CONTRACT);
    expect(decoded1.isGet());
    expect(decoded0.unwrapGet().state).toStrictEqual(
      decoded1.unwrapGet().state
    );
  });

  test("update notification deserialization", () => {
    const UPDATE_NOTIFICATION = new Uint8Array([
      129, 162, 79, 107, 129, 178, 85, 112, 100, 97, 116, 101, 78, 111, 116,
      105, 102, 105, 99, 97, 116, 105, 111, 110, 146, 146, 220, 0, 32, 26, 204,
      222, 111, 45, 67, 204, 248, 23, 49, 18, 204, 199, 204, 182, 25, 204, 145,
      204, 136, 26, 204, 137, 204, 131, 120, 125, 23, 204, 133, 204, 226, 204,
      230, 204, 215, 204, 240, 38, 61, 204, 159, 55, 81, 204, 199, 204, 206,
      220, 0, 32, 29, 122, 204, 214, 26, 26, 204, 232, 204, 173, 121, 204, 147,
      204, 247, 204, 138, 204, 177, 91, 204, 206, 204, 143, 204, 234, 204, 205,
      204, 188, 204, 222, 204, 190, 204, 165, 204, 187, 63, 65, 85, 44, 14, 204,
      251, 204, 228, 204, 149, 204, 232, 204, 183, 145, 204, 133,
    ]);
    let decoded = new HostResponse(UPDATE_NOTIFICATION);
    expect(decoded.isUpdateNotification());
  });
});

describe("locutus websocket API err result deserialization", () => {
  test("put request error", () => {
    const PUT_ERROR = new Uint8Array([
      129, 163, 69, 114, 114, 145, 129, 172, 82, 101, 113, 117, 101, 115, 116,
      69, 114, 114, 111, 114, 129, 163, 80, 117, 116, 146, 146, 220, 0, 32, 204,
      204, 204, 180, 105, 204, 200, 204, 233, 98, 97, 204, 193, 26, 60, 204,
      136, 98, 204, 131, 80, 121, 122, 204, 213, 204, 178, 98, 204, 225, 98,
      118, 104, 204, 183, 29, 68, 204, 222, 204, 133, 204, 155, 118, 75, 204,
      162, 220, 0, 32, 204, 227, 86, 204, 134, 46, 16, 97, 51, 204, 244, 127,
      101, 204, 152, 204, 196, 204, 221, 83, 4, 204, 223, 64, 107, 204, 246, 19,
      43, 95, 204, 142, 46, 204, 136, 204, 251, 113, 82, 204, 153, 68, 204, 231,
      204, 201, 172, 114, 97, 110, 100, 111, 109, 32, 99, 97, 117, 115, 101,
    ]);
    let decoded = new HostResponse(PUT_ERROR);
    expect(decoded.isErr());
  });

  test("update request error", () => {
    const UPDATE_ERROR = new Uint8Array([
      129, 163, 69, 114, 114, 145, 129, 172, 82, 101, 113, 117, 101, 115, 116,
      69, 114, 114, 111, 114, 129, 166, 85, 112, 100, 97, 116, 101, 146, 146,
      220, 0, 32, 97, 21, 204, 133, 204, 200, 21, 204, 133, 84, 58, 204, 255,
      204, 151, 0, 82, 70, 8, 107, 3, 204, 151, 30, 110, 204, 189, 204, 203, 68,
      125, 204, 173, 204, 175, 18, 114, 66, 204, 135, 42, 204, 237, 14, 220, 0,
      32, 204, 141, 0, 204, 130, 204, 144, 85, 204, 155, 204, 159, 87, 204, 166,
      91, 123, 204, 200, 204, 228, 32, 204, 179, 204, 248, 99, 61, 204, 229,
      204, 229, 90, 204, 226, 122, 104, 204, 151, 204, 227, 39, 12, 100, 204,
      199, 120, 204, 134, 172, 114, 97, 110, 100, 111, 109, 32, 99, 97, 117,
      115, 101,
    ]);
    let decoded = new HostResponse(UPDATE_ERROR);
    expect(decoded.isErr());
  });

  test("get request error", () => {
    const GET_ERROR = new Uint8Array([
      129, 163, 69, 114, 114, 145, 129, 172, 82, 101, 113, 117, 101, 115, 116,
      69, 114, 114, 111, 114, 129, 163, 71, 101, 116, 146, 146, 220, 0, 32, 90,
      204, 130, 204, 217, 34, 204, 212, 95, 99, 204, 140, 204, 162, 127, 204,
      239, 204, 149, 120, 204, 196, 86, 57, 104, 23, 204, 129, 121, 204, 224,
      204, 181, 115, 68, 204, 169, 204, 206, 21, 72, 95, 60, 117, 29, 220, 0,
      32, 204, 129, 204, 168, 204, 142, 18, 51, 204, 137, 111, 204, 152, 93, 48,
      204, 221, 204, 221, 204, 144, 33, 204, 144, 204, 248, 204, 245, 19, 103,
      39, 204, 177, 70, 204, 249, 204, 244, 96, 204, 138, 89, 204, 152, 36, 204,
      130, 1, 204, 212, 172, 114, 97, 110, 100, 111, 109, 32, 99, 97, 117, 115,
      101,
    ]);
    let decoded = new HostResponse(GET_ERROR);
    expect(decoded.isErr());
  });

  test("disconnect request error", () => {
    const DISCONNECT_ERROR = new Uint8Array([
      129, 163, 69, 114, 114, 145, 129, 172, 82, 101, 113, 117, 101, 115, 116,
      69, 114, 114, 111, 114, 170, 68, 105, 115, 99, 111, 110, 110, 101, 99,
      116,
    ]);
    let decoded = new HostResponse(DISCONNECT_ERROR);
    expect(decoded.isErr());
  });
});
