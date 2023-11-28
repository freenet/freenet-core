/**
 * @jest-environment node
 */

import * as flatbuffers from "flatbuffers";
import * as fbTopology from "../src/generated/topology";

test("Flatbuffer deserialization test", () => {
  //   // Create a sample flatbuffer data
  //   const builder = new flatbuffers.Builder();
  //   const nodeId = builder.createString("node1");
  //   const nodeType = fbTopology.;
  //   const nodeOffset = fbTopology.Node.createNode(builder, nodeId, nodeType);
  //   const nodesOffset = fbTopology.Topology.createNodesVector(builder, [
  //     nodeOffset,
  //   ]);
  //   fbTopology.Topology.startTopology(builder);
  //   fbTopology.Topology.addNodes(builder, nodesOffset);
  //   const topologyOffset = fbTopology.Topology.endTopology(builder);
  //   builder.finish(topologyOffset);

  //   // Deserialize the flatbuffer data
  //   const buffer = builder.asUint8Array();
  //   const topology = fbTopology.Topology.getRootAsTopology(
  //     new flatbuffers.ByteBuffer(buffer)
  //   );

  //   // Perform assertions on the deserialized data
  //   expect(topology.nodesLength()).toBe(1);
  //   const node = topology.nodes(0);
  //   expect(node.id()).toBe("node1");
  //   expect(node.type()).toBe(fbTopology.NodeType.Router);

  const AddedConnectionMessage = new Uint8Array([
    20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 12, 0, 0, 0, 7, 0, 8, 0, 10, 0, 0, 0,
    0, 0, 0, 1, 16, 0, 0, 0, 12, 0, 28, 0, 4, 0, 12, 0, 8, 0, 20, 0, 12, 0, 0,
    0, 84, 0, 0, 0, 20, 0, 0, 0, 182, 64, 79, 180, 180, 193, 216, 63, 27, 191,
    192, 101, 40, 220, 226, 63, 52, 0, 0, 0, 49, 50, 68, 51, 75, 111, 111, 87,
    69, 106, 87, 74, 50, 53, 118, 71, 121, 66, 77, 68, 76, 78, 99, 104, 65, 105,
    97, 84, 76, 121, 81, 99, 105, 50, 53, 65, 70, 65, 71, 122, 82, 49, 71, 57,
    113, 98, 52, 86, 115, 82, 122, 66, 0, 0, 0, 0, 52, 0, 0, 0, 49, 50, 68, 51,
    75, 111, 111, 87, 75, 77, 75, 102, 99, 66, 54, 74, 100, 81, 101, 72, 107,
    112, 120, 121, 86, 70, 49, 87, 85, 85, 103, 121, 115, 85, 85, 54, 119, 66,
    71, 97, 116, 50, 49, 84, 54, 51, 109, 109, 98, 90, 53, 117, 0, 0, 0, 0,
  ]);

  const buf = new flatbuffers.ByteBuffer(
    new Uint8Array(AddedConnectionMessage)
  );
  const peerChange = fbTopology.PeerChange.getRootAsPeerChange(buf);
});
