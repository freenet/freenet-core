import { local } from "d3";
import React, {useEffect} from "react";
import {createRoot} from "react-dom/client";
import {RingVisualizationPoint, RingVisualizationProps} from "./type_definitions";


export const RingVisualization = ({main_peer, other_peers}: RingVisualizationProps) => {
    const [peers, setPeers] = React.useState(true);
    const [contracts, setContracts] = React.useState(false);

    const togglePeers = () => {
        setPeers(!peers);
    }

    const toggleContracts = () => {
        setContracts(!contracts);
    }


    useEffect(() => {
        console.log("Peers visibility changed");
        console.log(peers);
    }, [peers]);

    useEffect(() => {
        console.log("Contracts visibility changed");
        console.log(contracts);
    }, [contracts]);


    // Declare the chart dimensions and margins.
    const width = 640;
    const height = 400;
    const marginTop = 20;
    const marginRight = 20;
    const marginBottom = 30;
    const marginLeft = 40;

    // // Declare the x (horizontal position) scale.
    // const x = d3
    //     .scaleUtc()
    //     .domain([new Date("2023-01-01"), new Date("2024-01-01")])
    //     .range([marginLeft, width - marginRight]);

    // // Declare the y (vertical position) scale.
    // const y = d3
    //     .scaleLinear()
    //     .domain([0, 100])
    //     .range([height - marginBottom, marginTop]);

    // // Create the SVG container.
    // const svg = d3.create("svg").attr("width", width).attr("height", height);

    // // Add the x-axis.
    // svg.append("g")
    //     .attr("transform", `translate(0,${height - marginBottom})`)
    //     .call(d3.axisBottom(x));

    // // Add the y-axis.
    // svg.append("g")
    //     .attr("transform", `translate(${marginLeft},0)`)
    //     .call(d3.axisLeft(y));
    //
    let scale = 3;


    let tooltip_x = 100;
    let tooltip_y = 100;

    let tooltips_visibility = {};

    const draw_points = (internal_other_peers: RingVisualizationPoint[]) => {
        return internal_other_peers.map((peer, index) => {
            let return_values = (
                <>
                    <circle
                        key={index}
                        cx={calculate_point(peer.localization, scale).x}
                        cy={calculate_point(peer.localization, scale).y}
                        r={1.2 * scale}
                        onMouseEnter={(e) => {
                            document.styleSheets[2].addRule(
                                `.svg-tooltip-${peer.localization
                                    .toString()
                                    .replace(".", "")}`,
                                "display: block"
                            );
                        }}
                        onMouseLeave={() => {
                            document.styleSheets[2].addRule(
                                `.svg-tooltip-${peer.localization
                                    .toString()
                                    .replace(".", "")}`,
                                "display: none"
                            );
                        }}
                    />

                    <text
                        id="svg-tooltip"
                        x={`${
                            calculate_point(peer.localization, scale).x + 10
                        }`}
                        y={`${
                            calculate_point(peer.localization, scale).y + 10
                        }`}
                        className={`svg-tooltip-${peer.localization
                            .toString()
                            .replace(".", "")}`}
                    >
                        {peer.peerId}: {peer.localization}
                    </text>
                </>
            );

            document.styleSheets[2].addRule(
                `.svg-tooltip-${peer.localization
                    .toString()
                    .replace(".", "")}`,
                "display: none"
            );
            return return_values;
        })
    }

    const draw_connecting_lines = (internal_other_peers: RingVisualizationPoint[]) => {

        return internal_other_peers.map((peer, index) => {
            let return_values = (
                <>
                    <path
                        className="line"
                        d={`M ${calculate_point(main_peer.localization, scale).x} ${
                            calculate_point(main_peer.localization, scale).y
                        } L ${calculate_point(peer.localization, scale).x} ${
                            calculate_point(peer.localization, scale).y
                        }`}

                        style={{strokeWidth:3}}


                        onMouseEnter={(e) => {
                            document.styleSheets[2].addRule(
                                `.svg-tooltip-distance-${peer.localization
                                    .toString()
                                    .replace(".", "")}`,
                                "display: block"
                            );
                        }}
                        onMouseLeave={() => {
                            document.styleSheets[2].addRule(
                                `.svg-tooltip-distance-${peer.localization
                                    .toString()
                                    .replace(".", "")}`,
                                "display: none"
                            );
                        }}
                    />

                    <text
                        id="svg-tooltip-distance"
                        x={`${
                            calculate_point(peer.localization, scale).x - 10
                        }`}
                        y={`${
                            calculate_point(peer.localization, scale).y - 10
                        }`}
                        className={`svg-tooltip-distance-${peer.localization
                            .toString()
                            .replace(".", "")}`}
                    >
                        distance: {Math.abs(peer.localization - main_peer.localization)}
                    </text>

                </>
            );


            document.styleSheets[2].addRule(
                `.svg-tooltip-distance-${peer.localization
                    .toString()
                    .replace(".", "")}`,
                "display: none"
            );

            return return_values;
        })
    }



    return (
        <div>
        <svg width={400} height={100 * scale} style={{ position: "relative" }}>
            <path
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                d={"20"}
            />
            <g fill="white" stroke="currentColor" strokeWidth="1.5">
                <circle
                    key={"123"}
                    cx={60 * scale}
                    cy={50 * scale}
                    r={40 * scale}
                />

                <circle
                    cx={calculate_point(main_peer.localization, scale).x}
                    cy={calculate_point(main_peer.localization, scale).y}
                    r={1.5 * scale}
                    fill="red"
                />

                {draw_points(other_peers)}

                {draw_connecting_lines(other_peers)}

                
            </g>
        </svg>
        <div>

            <button className="button" onClick={() => togglePeers()}>Peers</button>

            <button className="button" onClick={() => toggleContracts()}>Contracts</button></div>
        </div>
    );



};

const calculate_point = (localization: number, scale: number) => {
    return {
        x:
            Math.cos(2 * Math.PI * localization) * 40 * scale +
            40 * scale +
            20 * scale,
        y:
            Math.sin(2 * Math.PI * localization) * 40 * scale +
            40 * scale +
            10 * scale,
    };
};
