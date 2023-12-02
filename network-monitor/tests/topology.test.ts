/**
 * @jest-environment jsdom
 */

import {
  AddedConnectionT,
  RemovedConnectionT,
} from "../src/generated/topology";
import {
  handleAddedConnection,
  handleRemovedConnection,
  peers,
  PeerId,
} from "../src/topology";

describe("Network Monitor App", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  test("should update peers table when receiving added connection", () => {
    const mockAddedConnection = new AddedConnectionT(
      "tx1",
      "peer1",
      0.6254,
      "peer2",
      0.2875
    );
    handleAddedConnection(mockAddedConnection);

    const peer1 = new PeerId("peer1");
    const peer2 = new PeerId("peer2");
    expect(peers).toEqual({
      peer1: {
        id: peer1,
        locationHistory: [],
        currentLocation: 0.6254,
        connectionTimestamp: expect.any(Number),
        connections: [{ id: peer2, location: 0.2875, transaction: "tx1" }],
        history: [
          {
            type: "Added",
            from: {
              id: peer1,
              location: 0.6254,
              transaction: "tx1",
            },
            to: {
              id: peer2,
              location: 0.2875,
              transaction: "tx1",
            },
            timestamp: expect.any(Number),
          },
        ],
      },
      peer2: {
        id: peer2,
        locationHistory: [],
        currentLocation: 0.2875,
        connectionTimestamp: expect.any(Number),
        connections: [{ id: peer1, location: 0.6254, transaction: "tx1" }],
        history: [
          {
            type: "Added",
            from: {
              id: peer1,
              location: 0.6254,
              transaction: "tx1",
            },
            to: {
              id: peer2,
              location: 0.2875,
              transaction: "tx1",
            },
            timestamp: expect.any(Number),
          },
        ],
      },
    });
  });

  test("should update peers table when receiving removed connection", () => {
    const removedConnection = new RemovedConnectionT("peer1", "peer2");

    handleRemovedConnection(removedConnection);

    expect(peers["peer1"].connections).toHaveLength(0);
    expect(peers["peer2"].connections).toHaveLength(0);
    expect(peers["peer1"].history).toHaveLength(2);
    expect(peers["peer2"].history).toHaveLength(2);
  });
});
