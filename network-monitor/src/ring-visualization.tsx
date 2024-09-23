import { local } from "d3";
import React, {useEffect} from "react";
import {createRoot} from "react-dom/client";
import {RingVisualizationPoint, RingVisualizationProps} from "./type_definitions";


export const RingVisualization = ({main_peer, other_peers, selected_text = "Peer"}: RingVisualizationProps) => {
    const [selected_peer, setSelectedPeer] = React.useState<RingVisualizationPoint | undefined>(main_peer);

    useEffect(() => {
        console.log("Selected peer changed");
        setSelectedPeer(main_peer);
    }, [main_peer]);

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
                        fill="steelblue"
                        r={1.5 * scale}
                        
                        onMouseEnter={(e) => {
                            document.querySelector(`.svg-tooltip-${peer.peerId}`)!.removeAttribute("style");
                            document.querySelector(`.svg-tooltip-${peer.peerId}`)!.setAttribute("style", "display: block;");
                        }}
                        onMouseLeave={() => {
                            document.querySelector(`.svg-tooltip-${peer.peerId}`)!.removeAttribute("style");
                            document.querySelector(`.svg-tooltip-${peer.peerId}`)!.setAttribute("style", "display: none;");
                        }}
                        style={{zIndex: 999}}
                        onClick={() => {
                            setSelectedPeer(peer);
                        }}

                    />


                    <text
                        id="svg-tooltip"
                        x={`${
                            calculate_point(peer.localization, scale).x + (peer.localization < 0.7 && peer.localization > 0.35 ? -130 : 0) + (peer.localization > 0.9 || peer.localization < 0.15 ? 10 : 0)
                        }`}
                        y={`${
                            calculate_point(peer.localization, scale).y + (peer.localization > 0.7 && peer.localization < 0.9 ? -10 : 0) + (peer.localization > 0.15 && peer.localization < 0.35 ? 30 : 0)
                        }`}
                        className={`svg-tooltip-${peer.peerId}`}
                        style={{backgroundColor: "white", padding: 5, zIndex: 999, display: "none"}}
                        fontWeight={100}
                        fontSize={13}
                    >
                        {peer.peerId.slice(-8)}
                        @ {peer.localization.toString().slice(0, 8)}
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

    const draw_connecting_lines_from_main_peer_to_other_peers = (internal_other_peers: RingVisualizationPoint[]) => {
        if (!main_peer) {
            return <></>;
        }

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
                            // document.querySelector(selectors)

                            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.removeAttribute("style");
                            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.setAttribute("style", "display: block;");

                            // document.styleSheets[2].addRule(
                            //     `.svg-tooltip-distance-${peer.localization
                            //         .toString()
                            //         .replace(".", "")}`,
                            //     "display: block"
                            // );
                        }}
                        onMouseLeave={() => {
                            // document.querySelector(selectors)

                            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.removeAttribute("style");
                            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.setAttribute("style", "display: none;");

                            // document.styleSheets[2].addRule(
                            //     `.svg-tooltip-distance-${peer.localization
                            //         .toString()
                            //         .replace(".", "")}`,
                            //     "display: none"
                            // );

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

                        fontWeight={100}
                    >
                        distance: {Math.abs(peer.localization - main_peer.localization)}
                    </text>


                </>
            );


            // document.querySelector(selectors)


            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.removeAttribute("style");
            document.querySelector(`.svg-tooltip-distance-${peer.peerId}`)!.setAttribute("style", "display: none;");

            // document.styleSheets[2].addRule(
            //     `.svg-tooltip-distance-${peer.localization
            //         .toString()
            //         .replace(".", "")}`,
            //     "display: none"
            // );

            return return_values;
        })
    }

    const draw_main_peer_text = (main_peer: RingVisualizationPoint | undefined, scale: number) => {
        if (!main_peer) {
            return <></>;
        }
        return (
            <text
                // main peer
                id="svg-tooltip"
                x={`${
                    calculate_point(main_peer.localization, scale).x + 10
                }`}
                y={`${calculate_point(main_peer.localization, scale).y + (main_peer.localization > 0.64 && main_peer.localization < 0.9 ? -20 : 0) + (main_peer.localization > 0.15 && main_peer.localization < 0.35 ? 30 : 0)}`}
                className={`svg-tooltip-${main_peer.localization
                    .toString()
                    .replace(".", "")}`}
                fontWeight={100}
                onClick={() => {
                    setSelectedPeer(main_peer);
                }}
                style={{zIndex: 999}}
                
            >
                {main_peer.peerId} 
                 @ {main_peer.localization.toString().slice(0, 8)}

            </text>
        );
    };

    const draw_main_peer = (main_peer: RingVisualizationPoint | undefined, scale: number) => {
        if (!main_peer) {
            return <></>;
        }

        return (
            <circle
                cx={calculate_point(main_peer.localization, scale).x}
                cy={calculate_point(main_peer.localization, scale).y}
                r={1.8 * scale}
                fill="red"
                style={{zIndex: 999, padding: 10}}

            />
        );
    }

    const draw_selected_peer_text = (selected_peer: RingVisualizationPoint | undefined) => {
        if (!selected_peer) {
            return <></>;
        }
        return (
            <text 
                id="selected_peer"
                x={350}
                y={30}
                fontSize={14}
                fontWeight={100}
            >
                Selected {selected_text}: 
                {" "}{selected_peer.peerId.slice(-8)}
                @ {selected_peer.localization.toString().slice(0, 8)}
            </text>
        );
    }

    return (
        <div>
        <svg width={700} height={100 * scale} style={{ position: "relative" }}>
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

                {draw_main_peer(main_peer, scale)}

                {draw_connecting_lines_from_main_peer_to_other_peers(other_peers)}

                {draw_points(other_peers)}

                {draw_main_peer_text(main_peer, scale)}
                
                {draw_selected_peer_text(selected_peer)}


                
            </g>
        </svg>
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
