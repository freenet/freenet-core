import * as fbTopology from "./generated/topology";
import * as d3 from "d3";
import { BaseType } from "d3-selection";
import { createRoot } from "react-dom/client";
import { component } from "./react-init";

export let peers: PeerList = {};

interface PeerList {
    [id: string]: Peer;
}

interface Peer {
    id: PeerId;
    currentLocation: number;
    connectionTimestamp: number;
    connections: Connection[];
    history: ChangeInfo[];
    locationHistory: { location: number; timestamp: number }[];
}

interface Connection {
    transaction: string | null;
    id: PeerId;
    location: number;
}

interface ChangeInfo {
    type: "Added" | "Removed";
    from: Connection;
    to: Connection;
    timestamp: number;
}

export class PeerId {
    private id: string;

    constructor(id: string | Uint8Array) {
        if (id instanceof Uint8Array) {
            this.id = new TextDecoder().decode(id);
        } else {
            this.id = id;
        }
    }

    get short() {
        return this.id.slice(-8);
    }

    get full() {
        return this.id;
    }
}

export function handleChange(peerChange: fbTopology.PeerChange) {
    const previousPeers = Object.keys(peers).length;
    try {
        const unpacked = peerChange.unpack();
        switch (unpacked.changeType) {
            case fbTopology.PeerChangeType.AddedConnection:
                handleAddedConnection(
                    unpacked.change as fbTopology.AddedConnectionT
                );
                break;
            case fbTopology.PeerChangeType.RemovedConnection:
                handleRemovedConnection(
                    unpacked.change as fbTopology.RemovedConnectionT
                );
                break;
            case fbTopology.PeerChangeType.NONE:
                break;
            case fbTopology.PeerChangeType.Error:
                const error = unpacked.change as fbTopology.ErrorT;
                if (error.message) {
                    console.error(error.message);
                }
                break;
        }
        unpacked.currentState.forEach((connection) => {
            handleAddedConnection(connection, false);
        });
    } catch (e) {
        console.error(e);
    } finally {
        if (previousPeers !== Object.keys(peers).length) {
            ringHistogram(Object.values(peers));
            updateTable();
        }
        return true;
    }
}

export function handleAddedConnection(
    peerChange: fbTopology.AddedConnectionT,
    skipNonTransaction = true
) {
    if (!peerChange.transaction && skipNonTransaction) {
        // only add connections if they have been reported as part of a transaction
        // otherwise we end up with duplicates here
        return;
    }
    const added = peerChange;
    const from = new PeerId(added.from!);
    const to = new PeerId(added.to!);

    let transaction: string | null;
    if (typeof added.transaction === "string") {
        transaction = added.transaction;
    } else if (added.transaction instanceof Uint8Array) {
        transaction = new TextDecoder().decode(added.transaction);
    } else {
        transaction = null;
    }
    const fromConnection: Connection = {
        transaction: transaction,
        id: from,
        location: added.fromLocation,
    };

    const toConnection: Connection = {
        transaction: transaction,
        id: to,
        location: added.toLocation,
    };

    const fromFullId = from.full;
    if (peers[fromFullId]) {
        if (peers[fromFullId].currentLocation !== added.fromLocation) {
            peers[fromFullId].locationHistory.push({
                location: peers[fromFullId].currentLocation,
                timestamp: Date.now(),
            });
            peers[fromFullId].currentLocation = added.fromLocation;
        }

        if (
            !peers[fromFullId].connections.some(
                (conn) => conn.id.full === to.full
            )
        ) {
            peers[fromFullId].connections.push(toConnection);
        }
    } else {
        peers[fromFullId] = {
            id: from,
            currentLocation: added.fromLocation,
            connectionTimestamp: Date.now(),
            connections: [toConnection],
            history: [],
            locationHistory: [],
        };
    }

    const toFullId = to.full;
    if (peers[toFullId]) {
        if (peers[toFullId].currentLocation !== added.toLocation) {
            peers[toFullId].locationHistory.push({
                location: peers[toFullId].currentLocation,
                timestamp: Date.now(),
            });
            peers[toFullId].currentLocation = added.toLocation;
        }

        if (
            !peers[toFullId].connections.some(
                (conn) => conn.id.full === from.full
            )
        ) {
            peers[toFullId].connections.push(fromConnection);
        }
    } else {
        peers[toFullId] = {
            id: to,
            currentLocation: added.toLocation,
            connectionTimestamp: Date.now(),
            connections: [fromConnection],
            history: [],
            locationHistory: [],
        };
    }

    const changeInfo: ChangeInfo = {
        type: "Added",
        from: fromConnection,
        to: toConnection,
        timestamp: Date.now(),
    };

    // Check if the (to, from) pair or its reverse is already present in the history
    const isPresent = peers[fromFullId].history.some(
        (item) =>
            (item.from.id.full === changeInfo.from.id.full &&
                item.to.id.full === changeInfo.to.id.full &&
                item.from.transaction === changeInfo.from.transaction) ||
            (item.from.id.full === changeInfo.to.id.full &&
                item.to.id.full === changeInfo.from.id.full &&
                item.from.transaction === changeInfo.from.transaction)
    );

    // Only push changeInfo if the pair is not already present
    if (!isPresent) {
        peers[fromFullId].history.push(changeInfo);
        peers[toFullId].history.push(changeInfo);
    }
}

export function handleRemovedConnection(
    peerChange: fbTopology.RemovedConnectionT
) {
    const from = new PeerId(peerChange.from!);
    const at = new PeerId(peerChange.at!);

    const index = peers[from.full].connections.findIndex(
        (connection) => connection.id.full === at.full
    );

    if (index > -1) {
        peers[from.full].connections.splice(index, 1);
    }

    const reverseIndex = peers[at.full].connections.findIndex(
        (connection: Connection) => connection.id.full === from.full
    );

    if (reverseIndex > -1) {
        peers[at.full].connections.splice(reverseIndex, 1);
    }

    const changeInfo: ChangeInfo = {
        type: "Removed",
        from: {
            transaction: null,
            id: from,
            location: peers[from.full].currentLocation,
        },
        to: {
            transaction: null,
            id: at,
            location: peers[at.full].currentLocation,
        },
        timestamp: Date.now(),
    };

    peers[from.full].history.push(changeInfo);
    peers[at.full].history.push(changeInfo);
}

function updateTable() {
    const peerConnectionsDiv = document.getElementById("peer-details")!;

    const table = document.getElementById("peers-table")!;

    const tbody = table.querySelector("tbody")!;
    tbody.innerHTML = "";

    const setDivPosition = (event: MouseEvent) => {
        peerConnectionsDiv.style.display = "block";
        const rect = peerConnectionsDiv.offsetParent!.getBoundingClientRect();
        const divHeight = peerConnectionsDiv.offsetHeight;

        // Check if the div would render off the bottom of the screen
        if (event.clientY + divHeight > window.innerHeight) {
            // If so, position it above the mouse cursor instead
            peerConnectionsDiv.style.top = `${
                event.clientY - rect.top - divHeight
            }px`;
        } else {
            // Otherwise, position it below the mouse cursor as usual
            peerConnectionsDiv.style.top = `${event.clientY - rect.top}px`;
        }

        peerConnectionsDiv.style.left = `${event.clientX - rect.left + 15}px`;
    };

    for (const peer in peers) {
        const peerData = peers[peer];
        const row = document.createElement("tr");
        row.addEventListener("mouseover", (event) => {
            setDivPosition(event);
            showPeerData(peers[peer]);
        });
        row.addEventListener("mousemove", (event) => {
            setDivPosition(event);
        });
        row.addEventListener(
            "mouseout",
            () => (peerConnectionsDiv.style.display = "none")
        );
        row.addEventListener("click", () => {
            const modal = document.createElement("div");
            modal.classList.add("modal", "is-active");
            const modalBackground = document.createElement("div");
            modalBackground.classList.add("modal-background");
            modal.appendChild(modalBackground);

            const modalContent = document.createElement("div");
            modalContent.classList.add("modal-content");
            // Set the width of the modal content based on the viewport width
            if (window.innerWidth >= 1200) {
                modalContent.style.width = "80%";
            } else if (window.innerWidth >= 769) {
                modalContent.style.width = "640px";
            } else {
                modalContent.style.width = "100%";
            }

            const modalBox = document.createElement("div");
            modalBox.classList.add("box", "column");
            modalBox.style.overflowWrap = "break-word";
            modalBox.style.whiteSpace = "normal";

            const currentDetails =
                document.getElementById("peer-details")!.innerHTML;
            modalBox.innerHTML = currentDetails;
            // Make the table width 100% to ensure it fits within the modal box
            modalBox.querySelectorAll("table").forEach((table) => {
                if (table) {
                    table.style.width = "100%";
                    table.style.tableLayout = "fixed";
                }
            });

            modalContent.appendChild(modalBox);
            modal.appendChild(modalContent);

            const closeModal = document.createElement("button");
            closeModal.classList.add("modal-close", "is-large");
            modal.appendChild(closeModal);

            const mainDiv = document.querySelector(".main")!;
            mainDiv.appendChild(modal);
            modal.addEventListener("click", () => {
                modal.style.display = "none";
                const graphContainer = d3.select("#peer-conns-graph");
                graphContainer.selectAll("*").remove();
                modal.remove();
            });

            // Build the neightbours information section
            const neightboursDiv = document.createElement("div");
            neightboursDiv.classList.add("columns");

            const graphDiv = document.createElement("div");
            graphDiv.id = "peer-conns-graph";
            graphDiv.classList.add("column");
            graphDiv.style.display = "flex";
            graphDiv.style.justifyContent = "center";
            graphDiv.style.alignItems = "center";
            const scaleFactor = 1.25; // Replace this with your actual scale factor
            graphDiv.style.padding = `${10 * scaleFactor}px`; // Replace 10 with your actual padding
            neightboursDiv.appendChild(graphDiv);

            const peerConnectionsT = modalBox.querySelector(
                "#peer-connections-t"
            )!;
            peerConnectionsT.classList.add("column");
            neightboursDiv.appendChild(peerConnectionsT);

            const graphContainer = d3.select(graphDiv);
            ringVisualization(peerData, graphContainer, scaleFactor);

            // Insert the new div before peer-connections-t in modalBox
            modalBox
                .querySelector("#neighbours-t")!
                .insertAdjacentElement("afterend", neightboursDiv);
        });

        const id = document.createElement("td");
        id.textContent = "..." + peerData.id.short;
        const location = document.createElement("td");
        location.textContent = peerData.currentLocation.toString();
        const connectionTimestamp = document.createElement("td");
        const timestamp = new Date(peerData.connectionTimestamp);
        connectionTimestamp.textContent = `${timestamp.toUTCString()} (${timestamp.getMilliseconds()}ms)`;
        row.appendChild(id);
        row.appendChild(location);
        row.appendChild(connectionTimestamp);

        // Add event listeners to each td element
        const tds = row.getElementsByTagName("td");
        for (let i = 0; i < tds.length; i++) {
            tds[i].addEventListener("mouseover", (event) => {
                setDivPosition(event);
                showPeerData(peers[peer]);
            });
            row.addEventListener("mousemove", (event) => {
                setDivPosition(event);
            });
            tds[i].addEventListener(
                "mouseout",
                () => (peerConnectionsDiv.style.display = "none")
            );
        }

        tbody.appendChild(row);
    }

    const rows = Array.from(tbody.querySelectorAll("tr"));
    const sortedRows = rows.sort((a, b) => {
        const cellA = a.cells[2].textContent!;
        const cellB = b.cells[2].textContent!;
        return cellA.localeCompare(cellB);
    });

    rows.forEach((row) => tbody.removeChild(row));
    sortedRows.forEach((row) => tbody.appendChild(row));
}

const sortDirections: number[] = [];

document.addEventListener("DOMContentLoaded", () => {
    ringHistogram(Object.values(peers));

    document
        .querySelector("#peers-table-h")!
        .querySelectorAll("th")!
        .forEach((header, index) => {
            sortDirections.push(1);
            tableSorting(header, index);
        });

    const root = createRoot(document.getElementById("react-init-point")!);
    root.render(component);

    (window as any).resetPeersTable = function () {
        peers = {};
        updateTable();
        ringHistogram(Object.values(peers));
    };
});

function tableSorting(header: HTMLTableCellElement, index: number) {
    header.addEventListener("click", () => {
        const tbody =
            header.parentElement!.parentElement!.parentElement!.querySelector(
                "tbody"
            )!;
        const rows = Array.from(tbody.querySelectorAll("tr"));

        const sortedRows = rows.sort((a, b) => {
            const cellA = a.cells[index].textContent!;
            const cellB = b.cells[index].textContent!;

            // Use a locale-sensitive string comparison for proper sorting
            // Multiply by the sort direction to toggle between ascending and descending order
            return cellA.localeCompare(cellB) * sortDirections[index];
        });

        // Toggle the sort direction for the next click
        sortDirections[index] = -sortDirections[index];
        const icon = header.querySelector("i")!;
        if (icon.classList.contains("fa-sort-amount-down")) {
            icon.classList.remove("fa-sort-amount-down");
            icon.classList.add("fa-sort-amount-up");
        } else {
            icon.classList.remove("fa-sort-amount-up");
            icon.classList.add("fa-sort-amount-down");
        }

        rows.forEach((row) => tbody.removeChild(row));
        sortedRows.forEach((row) => tbody.appendChild(row));
    });
}

export function showPeerData(peer: Peer) {
    const id = peer.id;
    peer.connections.sort((a, b) => a.location - b.location);

    // Set title
    const peerDataHeader = document.getElementById("peer-details-h")!;
    peerDataHeader.innerHTML = `
  <div class="block">
    <b>Peer Id</b>: ${id.full}</br>
    <b>Location</b>: ${peer.currentLocation ?? ""}
  </div>
  `;

    listPeerConnections(peer.connections);
    displayHistory(peer);
}

function ringVisualization(
    peer: Peer,
    graphContainer: d3.Selection<any, any, any, any>,
    scaleFactor: number
) {
    const width = 200;
    const height = 200;

    const svg = graphContainer
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .attr("style", "max-width: 100%")
        .attr("transform", `scale(${scaleFactor})`);

    // Set up the ring
    const radius = 75;
    const centerX = width / 2;
    const centerY = height / 2;

    const referencePoint = {
        value: peer.currentLocation,
        legend: peer.id.short,
    };
    const dataPoints = peer.connections.map((conn) => {
        return { value: conn.location, legend: conn.id.short };
    });

    svg.append("circle")
        .attr("cx", centerX)
        .attr("cy", centerY)
        .attr("r", radius)
        .attr("stroke", "black")
        .attr("stroke-width", 2)
        .attr("fill", "none");

    const tooltip = graphContainer
        .append("div")
        .attr("class", "tooltip")
        .style("position", "absolute")
        .style("visibility", "hidden")
        .style("background-color", "white")
        .style("border", "solid")
        .style("border-width", "2px")
        .style("border-radius", "5px")
        .style("padding", "5px");

    // Create points along the ring with legends
    svg.append("circle")
        .attr("class", "fixed-point")
        .attr(
            "cx",
            centerX + radius * Math.cos(referencePoint.value * 2 * Math.PI)
        )
        .attr(
            "cy",
            centerY + radius * Math.sin(referencePoint.value * 2 * Math.PI)
        )
        .attr("r", 5)
        .attr("fill", "red");

    function showLegend(
        this: SVGCircleElement,
        e: MouseEvent,
        d: { value: number; legend: string }
    ) {
        tooltip
            .style("visibility", "visible")
            .style("position", "absolute")
            .style("stroke", "black")
            .html(`<b>${d.legend}</b>: ${d.value}`);
        const tooltipRect = tooltip.node()!.getBoundingClientRect();
        tooltip
            .style("left", e.clientX - tooltipRect.width / 2 + "px")
            .style("top", e.clientY - tooltipRect.height / 2 + "px");
        d3.select(this).style("stroke", "black");
    }

    function hideLegend(this: SVGCircleElement) {
        tooltip.style("visibility", "hidden");
        d3.select(this).style("stroke", "none");
    }

    function circleId(d: { value: number; legend: string }, i: number) {
        return `circle-${i}-${d.legend}`;
    }

    svg.selectAll(".point")
        .data(dataPoints)
        .enter()
        .append("circle")
        .attr("class", "point")
        .attr("id", (d, i) => circleId(d, i))
        .attr("cx", (d) => centerX + radius * Math.cos(d.value * 2 * Math.PI))
        .attr("cy", (d) => centerY + radius * Math.sin(d.value * 2 * Math.PI))
        .attr("r", 5)
        .attr("fill", "steelblue")
        .on("mouseover", showLegend)
        .on("mousemove", showLegend)
        .on("mouseout", hideLegend);

    // Connect all points to the fixed point (0.3) with distances as tooltips

    function showDistance(
        this: SVGLineElement,
        e: MouseEvent,
        d: { value: number; legend: string }
    ) {
        const distance = getDistance(referencePoint.value, d.value).toFixed(5);
        tooltip
            .style("visibility", "visible")
            .style("position", "absolute")
            .style("z-index", "2")
            .html(`<b>Distance</b>: ${distance}`);
        const tooltipRect = tooltip.node()!.getBoundingClientRect();
        tooltip
            .style("left", e.clientX - tooltipRect.width / 2 + "px")
            .style("top", e.clientY - tooltipRect.height / 2 + "px");
        d3.select(this).style("stroke", "black");
    }

    function hideDistance(this: SVGLineElement) {
        tooltip.style("visibility", "hidden");
        d3.select(this).style("stroke", "gray");
    }

    function getDistance(point1: number, point2: number) {
        const diff = Math.abs(point1 - point2);
        return Math.min(diff, 1 - diff);
    }

    svg.selectAll(".edge")
        .data(dataPoints)
        .enter()
        .append("line")
        .attr("class", "edge")
        .attr(
            "x1",
            centerX + radius * Math.cos(referencePoint.value * 2 * Math.PI)
        )
        .attr(
            "y1",
            centerY + radius * Math.sin(referencePoint.value * 2 * Math.PI)
        )
        .attr("x2", (d) => centerX + radius * Math.cos(d.value * 2 * Math.PI))
        .attr("y2", (d) => centerY + radius * Math.sin(d.value * 2 * Math.PI))
        .attr("stroke", "gray")
        .attr("stroke-width", 1.5)
        .on("mouseover", showDistance)
        .on("mousemove", showDistance)
        .on("mouseout", hideDistance);
}

function listPeerConnections(connections: Connection[]) {
    // Find the existing peer connections table
    const tableBody = document.getElementById("peer-connections-b")!;

    // Clear the existing table rows
    while (tableBody.firstChild) {
        tableBody.removeChild(tableBody.firstChild);
    }

    // Create the table header row
    const headerRow = document.createElement("tr");
    const idHeader = document.createElement("th");
    idHeader.textContent = "Neighbour Id";
    const locationHeader = document.createElement("th");
    locationHeader.textContent = "Location";
    headerRow.appendChild(idHeader);
    headerRow.appendChild(locationHeader);
    tableBody.appendChild(headerRow);

    // Create and append the table rows for all peers
    connections.forEach((connection) => {
        const row = document.createElement("tr");
        const idCell = document.createElement("td");
        idCell.textContent = "..." + connection.id.short;
        const locationCell = document.createElement("td");
        locationCell.textContent = connection.location.toString() ?? "";
        row.appendChild(idCell);
        row.appendChild(locationCell);
        tableBody.appendChild(row);
    });
}

function displayHistory(peer: Peer) {
    const peerDetails = document.getElementById("peer-details")!;

    // Remove the existing table if it exists
    const existingTable = peerDetails.querySelector("#connection-history");
    if (existingTable) {
        existingTable.remove();
    }

    // Create a new table
    const table = document.createElement("table");
    table.id = "connection-history";
    table.classList.add("table", "is-striped", "block", "is-bordered");
    table.style.overflowWrap = "break-word";

    // Create the table header row
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    const typeHeader = document.createElement("th");
    typeHeader.textContent = "Type";
    const fromHeader = document.createElement("th");
    fromHeader.textContent = "From";
    const toHeader = document.createElement("th");
    toHeader.textContent = "To";
    const dateHeader = document.createElement("th");
    dateHeader.textContent = "Date";
    const transaction = document.createElement("th");
    transaction.textContent = "Transaction";
    headerRow.appendChild(typeHeader);
    headerRow.appendChild(fromHeader);
    headerRow.appendChild(toHeader);
    headerRow.appendChild(dateHeader);
    headerRow.appendChild(transaction);
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create the table body
    const tbody = document.createElement("tbody");
    const historyRows = peer.history.map((change) => {
        const row = document.createElement("tr");
        const typeCell = document.createElement("td");
        typeCell.textContent = change.type;
        const fromCell = document.createElement("td");
        fromCell.textContent = "..." + change.from.id.short; // Show last 8 characters
        const toCell = document.createElement("td");
        toCell.textContent = "..." + change.to.id.short; // Show last 8 characters
        const dateColumn = document.createElement("td");
        const date = new Date(change.timestamp);
        dateColumn.textContent = `${date.toUTCString()} (${date.getMilliseconds()}ms)`;
        const transactionCell = document.createElement("td");
        transactionCell.textContent = change.from.transaction
            ? change.from.transaction
            : "";
        row.appendChild(typeCell);
        row.appendChild(fromCell);
        row.appendChild(toCell);
        row.appendChild(dateColumn);
        row.appendChild(transactionCell);
        return row;
    });
    historyRows.forEach((row) => {
        tbody.appendChild(row);
    });
    table.appendChild(tbody);

    // Append the new table to the peerConnections element
    peerDetails.appendChild(table);
}

function ringHistogram(peerLocations: Peer[]) {
    const width = 500;
    const height = 300;
    const margin = { top: 10, right: 30, bottom: 50, left: 60 };
    const innerWidth = width - margin.left - margin.right;

    const container = d3.select("#peers-histogram");
    container.selectAll("*").remove();

    const svg = container
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("transform", `translate(${margin.left}, ${margin.top})`);

    const bucketSize = 0.05;

    const binsData: number[] = Array(Math.ceil(1 / bucketSize)).fill(0);
    peerLocations.forEach((peer) => {
        const binIndex = Math.floor(peer.currentLocation / bucketSize);
        binsData[binIndex]++;
    });

    const histogram = binsData.map((count, i) => ({
        x0: i * bucketSize,
        x1: (i + 1) * bucketSize,
        count,
    }));

    const xScale = d3.scaleLinear().domain([0, 1]).range([0, innerWidth]);
    const legendSpace = 50;
    const adjustedHeight = height - legendSpace;
    const padding = 10;
    const yScale = d3
        .scaleLinear()
        .domain([0, d3.max(histogram, (d) => d.count)! + padding])
        .range([adjustedHeight, 0]);

    const colorScale = d3
        .scaleQuantile<string>()
        .domain(binsData)
        .range(d3.schemeYlGn[9]);

    const bins = svg
        .selectAll("rect")
        .data(histogram)
        .enter()
        .append("rect")
        .attr("x", (d) => xScale(d.x0!))
        .attr("y", (d) => yScale(d.count))
        .attr("width", innerWidth / histogram.length)
        .attr("height", (d) => adjustedHeight - yScale(d.count))
        .attr("fill", (d) => colorScale(d.count))
        .attr("stroke", "black")
        .attr("stroke-width", 1);

    const tooltip = container
        .append("div")
        .attr("class", "bin-tooltip")
        .style("background-color", "white")
        .style("border", "solid")
        .style("border-width", "2px")
        .style("border-radius", "5px")
        .style("padding", "5px")
        .style("opacity", 0)
        .style("position", "absolute");

    bins.on("mouseover", (event: MouseEvent, d) => {
        tooltip.transition().duration(200).style("opacity", 0.9);
        tooltip
            .html(
                `Number of peers: ${d.count}, Subrange: [${d.x0.toFixed(
                    2
                )}, ${d.x1.toFixed(2)})`
            )
            .style("left", event.clientX + window.scrollX + "px")
            .style("top", event.clientY + window.scrollY - 150 + "px");
    })
        .on("mousemove", (event, _) => {
            tooltip
                .style("left", event.clientX + window.scrollX + "px")
                .style("top", event.clientY + window.scrollY - 150 + "px");
        })
        .on("mouseout", (_) => {
            tooltip.transition().duration(500).style("opacity", 0);
        });

    const xAxisTicks = Math.floor(innerWidth / 50); // 50 is the desired space between ticks
    const xAxis = d3.axisBottom(xScale).ticks(xAxisTicks);
    svg.append("g")
        .attr("transform", `translate(0, ${adjustedHeight})`)
        .call(xAxis);

    const yAxisTicks = Math.floor(adjustedHeight / 50); // 50 is the desired space between ticks
    const yAxis = d3.axisLeft(yScale).ticks(yAxisTicks);
    svg.append("g").call(yAxis);

    // Position the legend within the SVG area
    svg.append("text")
        .attr("fill", "#000")
        .attr("x", innerWidth / 2)
        .attr("y", height - margin.bottom / 4)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Peer Locations");

    svg.append("text")
        .attr("fill", "#000")
        .attr("y", margin.left - 100)
        .attr("x", -adjustedHeight / 2)
        .attr("transform", "rotate(-90)")
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Amount of Peers");
}
