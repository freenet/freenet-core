"use strict";
var __createBinding =
  (this && this.__createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);
        if (
          !desc ||
          ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)
        ) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }
        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });
var __setModuleDefault =
  (this && this.__setModuleDefault) ||
  (Object.create
    ? function (o, v) {
        Object.defineProperty(o, "default", { enumerable: true, value: v });
      }
    : function (o, v) {
        o["default"] = v;
      });
var __importStar =
  (this && this.__importStar) ||
  function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null)
      for (var k in mod)
        if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k))
          __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
  };
Object.defineProperty(exports, "__esModule", { value: true });
exports.showConnections =
  exports.handleRemovedConnection =
  exports.handleAddedConnection =
  exports.handleChange =
  exports.peers =
    void 0;
const fbTopology = __importStar(require("./generated/topology"));
exports.peers = {};
function handleChange(peerChange) {
  const unpacked = peerChange.unpack();
  try {
    switch (unpacked.changeType) {
      case fbTopology.PeerChangeType.AddedConnection:
        handleAddedConnection(unpacked.change);
        break;
      case fbTopology.PeerChangeType.RemovedConnection:
        handleRemovedConnection(unpacked.change);
        break;
    }
  } catch (e) {
    console.error(e);
  }
  updateTable();
}
exports.handleChange = handleChange;
function handleAddedConnection(peerChange) {
  const added = peerChange;
  const fromAdded = added.from.toString();
  const toAdded = added.to.toString();
  const fromConnection = {
    id: fromAdded,
    location: added.fromLocation,
  };
  const toConnection = {
    id: toAdded,
    location: added.toLocation,
  };
  if (exports.peers[fromAdded]) {
    if (exports.peers[fromAdded].currentLocation !== added.fromLocation) {
      exports.peers[fromAdded].locationHistory.push({
        location: exports.peers[fromAdded].currentLocation,
        timestamp: Date.now(),
      });
      exports.peers[fromAdded].currentLocation = added.fromLocation;
    }
    if (exports.peers[toAdded].currentLocation !== added.toLocation) {
      exports.peers[toAdded].locationHistory.push({
        location: exports.peers[toAdded].currentLocation,
        timestamp: Date.now(),
      });
      exports.peers[toAdded].currentLocation = added.toLocation;
    }
    exports.peers[fromAdded].connections.push(toConnection);
  } else {
    exports.peers[fromAdded] = {
      id: fromAdded,
      currentLocation: added.fromLocation,
      connections: [toConnection],
      history: [],
      locationHistory: [],
    };
  }
  if (exports.peers[toAdded]) {
    exports.peers[toAdded].connections.push(fromConnection);
  } else {
    exports.peers[toAdded] = {
      id: toAdded,
      currentLocation: added.toLocation,
      connections: [fromConnection],
      history: [],
      locationHistory: [],
    };
  }
  const changeInfo = {
    type: "Added",
    from: fromConnection,
    to: toConnection,
    timestamp: Date.now(),
  };
  exports.peers[fromAdded].history.push(changeInfo);
  exports.peers[toAdded].history.push(changeInfo);
}
exports.handleAddedConnection = handleAddedConnection;
function handleRemovedConnection(peerChange) {
  const removed = peerChange;
  const fromRemoved = removed.from.toString();
  const atRemoved = removed.at.toString();
  const index = exports.peers[fromRemoved].connections.findIndex(
    (connection) => connection.id === atRemoved
  );
  if (index > -1) {
    exports.peers[fromRemoved].connections.splice(index, 1);
  }
  const reverseIndex = exports.peers[atRemoved].connections.findIndex(
    (connection) => connection.id === fromRemoved
  );
  if (reverseIndex > -1) {
    exports.peers[atRemoved].connections.splice(reverseIndex, 1);
  }
  const changeInfo = {
    type: "Removed",
    from: {
      id: fromRemoved,
      location: exports.peers[fromRemoved].currentLocation,
    },
    to: {
      id: atRemoved,
      location: exports.peers[atRemoved].currentLocation,
    },
    timestamp: Date.now(),
  };
  exports.peers[fromRemoved].history.push(changeInfo);
  exports.peers[atRemoved].history.push(changeInfo);
}
exports.handleRemovedConnection = handleRemovedConnection;
function updateTable() {
  const table = document.getElementById("peers-table");
  if (table) {
    table.innerHTML = "";
    for (const id in exports.peers) {
      const row = document.createElement("tr");
      row.onclick = () => showConnections(exports.peers[id]);
      row.onclick = () => displayHistory(exports.peers[id]);
      const cell = document.createElement("td");
      cell.textContent = id;
      row.appendChild(cell);
      table.appendChild(row);
    }
  }
}
function showConnections(peer) {
  var _a, _b;
  const id = peer.id;
  const connections =
    (_a = peer.connections) !== null && _a !== void 0 ? _a : [];
  const overlayDiv = document.getElementById("overlay-div");
  if (overlayDiv) {
    overlayDiv.innerHTML = "";
    const headerSection = document.createElement("section");
    headerSection.classList.add("header-section");
    const idLocation = document.createElement("h2");
    idLocation.textContent = `Peer ID: ${id}, Location: ${
      (_b = peer.currentLocation) !== null && _b !== void 0 ? _b : ""
    }`;
    headerSection.appendChild(idLocation);
    overlayDiv.appendChild(headerSection);
    const tableSection = document.createElement("section");
    tableSection.classList.add("table-section");
    const table = document.createElement("table");
    table.classList.add("peers-table");
    // Create the table header row
    const headerRow = document.createElement("tr");
    const idHeader = document.createElement("th");
    idHeader.textContent = "Peer ID";
    const locationHeader = document.createElement("th");
    locationHeader.textContent = "Location";
    headerRow.appendChild(idHeader);
    headerRow.appendChild(locationHeader);
    table.appendChild(headerRow);
    // Create and append the table rows for all peers
    connections.forEach((connection) => {
      var _a, _b;
      const row = document.createElement("tr");
      const idCell = document.createElement("td");
      idCell.textContent =
        (_a = connection.id.toString()) !== null && _a !== void 0 ? _a : "";
      const locationCell = document.createElement("td");
      locationCell.textContent =
        (_b = connection.location.toString()) !== null && _b !== void 0
          ? _b
          : "";
      row.appendChild(idCell);
      row.appendChild(locationCell);
      table.appendChild(row);
    });
    // Append the table to the table section
    tableSection.appendChild(table);
    // Append the table section to the overlay div
    overlayDiv.appendChild(tableSection);
  }
}
exports.showConnections = showConnections;
function displayHistory(peer) {
  let historyHTML = '<table class="table is-fullwidth is-striped">';
  historyHTML +=
    "<thead><tr><th>Timestamp</th><th>Type</th><th>From</th><th>To</th></tr></thead><tbody>";
  historyHTML += peer.history
    .map((change) => {
      return `<tr><td>${change.timestamp}</td><td>${change.type}</td><td>${change.from.id}</td><td>${change.to.id}</td></tr>`;
    })
    .join("");
  historyHTML += "</tbody></table>";
  return historyHTML;
}
//# sourceMappingURL=topology.js.map
