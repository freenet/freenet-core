import {local} from "d3";

export const another_ring_visualization = () => {
    // Declare the chart dimensions and margins.
    const width = 640;
    const height = 400;
    const marginTop = 20;
    const marginRight = 20;
    const marginBottom = 30;
    const marginLeft = 40;
    
    const localizations = [0, 0.3, 0.865, 0.708, 0.5];

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
    console.log(calculate_point(localizations[0]));
    console.log(calculate_point(localizations[1]));

    let a = (
        <svg width={300} height={100}>
            <path
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                d={"20"}
            />
            <g fill="white" stroke="currentColor" strokeWidth="1.5">
                <circle key={"123"} cx={60} cy={50} r="40" />



                <circle cx={calculate_point(localizations[0]).x} cy={calculate_point(localizations[0]).y} r="2.5" fill="red" />

                <circle cx={calculate_point(localizations[1]).x} cy={calculate_point(localizations[1]).y} r="2.5" />

                <circle cx={calculate_point(localizations[2]).x} cy={calculate_point(localizations[2]).y} r="2.5" />

                <circle cx={calculate_point(localizations[3]).x} cy={calculate_point(localizations[3]).y} r="2.5" />

                <circle cx={calculate_point(localizations[4]).x} cy={calculate_point(localizations[4]).y} r="2.5" />

                <path d={`M ${calculate_point(localizations[0]).x} ${calculate_point(localizations[0]).y} L ${calculate_point(localizations[2]).x} ${calculate_point(localizations[2]).y}`} />

                <path d={`M ${calculate_point(localizations[0]).x} ${calculate_point(localizations[0]).y} L ${calculate_point(localizations[3]).x} ${calculate_point(localizations[3]).y}`} />
                
                <path d={`M ${calculate_point(localizations[0]).x} ${calculate_point(localizations[0]).y} L ${calculate_point(localizations[1]).x} ${calculate_point(localizations[1]).y}`} />

            </g>
        </svg>
    );

    // Append the SVG element.
    return a;
};

const calculate_point = (localization: number) => {
    return {
        x: ( Math.cos(2 * Math.PI * localization) * 40 ) + 40 + 20,
        y: ( Math.sin(2 * Math.PI * localization) * 40 ) + 40 + 10,
    };
}

