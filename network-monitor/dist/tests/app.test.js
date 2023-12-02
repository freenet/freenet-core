"use strict";
/**
 * @jest-environment jsdom
 */
Object.defineProperty(exports, "__esModule", { value: true });
const topology_1 = require("../src/generated/topology");
const topology_2 = require("../src/topology");
describe("Network Monitor App", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });
  test("should update peers table when receiving added connection", () => {
    const mockAddedConnection = new topology_1.AddedConnectionT(
      "peer1",
      0.6254,
      "peer2",
      0.2875
    );
    (0, topology_2.handleAddedConnection)(mockAddedConnection);
    expect(topology_2.peers).toEqual({
      peer1: {
        id: "peer1",
        locationHistory: [],
        currentLocation: 0.6254,
        connections: [{ id: "peer2", location: 0.2875 }],
        history: [
          {
            type: "Added",
            from: {
              id: "peer1",
              location: 0.6254,
            },
            to: {
              id: "peer2",
              location: 0.2875,
            },
            timestamp: expect.any(Number),
          },
        ],
      },
      peer2: {
        id: "peer2",
        locationHistory: [],
        currentLocation: 0.2875,
        connections: [{ id: "peer1", location: 0.6254 }],
        history: [
          {
            type: "Added",
            from: {
              id: "peer1",
              location: 0.6254,
            },
            to: {
              id: "peer2",
              location: 0.2875,
            },
            timestamp: expect.any(Number),
          },
        ],
      },
    });
  });
  test("should update peers table when receiving removed connection", () => {
    const removedConnection = new topology_1.RemovedConnectionT(
      "peer1",
      "peer2"
    );
    (0, topology_2.handleRemovedConnection)(removedConnection);
    expect(topology_2.peers["peer1"].connections).toHaveLength(0);
    expect(topology_2.peers["peer2"].connections).toHaveLength(0);
    expect(topology_2.peers["peer1"].history).toHaveLength(2);
    expect(topology_2.peers["peer2"].history).toHaveLength(2);
  });
});
//# sourceMappingURL=app.test.js.map
