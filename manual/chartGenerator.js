export const generateChartHtml = (item, chartTitle, chartContainerId, metadata) => {
    if (!item.dates || !item.counts || item.dates.length !== item.counts.length) {
        throw new Error(`Invalid data provided for chart generation: dates=${JSON.stringify(item.dates)}, counts=${JSON.stringify(item.counts)}`);
    }

    // Prepare data for Plotly using UTC dates
    const combinedData = item.dates.map((dateStr, idx) => {
        const [year, month, day] = dateStr.split('-').map(Number);
        const dateObj = new Date(Date.UTC(year, month - 1, 1));
        return { x: dateObj, y: item.counts[idx] };
    });

    const comparisonMean = parseFloat(item.comparison_mean.toFixed(1));
    const comparisonStdDev = parseFloat(item.comparison_stdDev.toFixed(1));

    // Set the start and end dates based on the comparison period and recent period
    const comparisonStartDate = new Date(Date.UTC(...metadata.comparisonPeriod.start.split('-').map(Number)));
    const recentEndDate = new Date(Date.UTC(...metadata.recentPeriod.end.split('-').map(Number)));

    // Filter out data outside of the specified date range to avoid white space
    const filteredData = combinedData.filter(point => point.x >= comparisonStartDate && point.x <= recentEndDate);

    // Prepare comparison and recent data traces based on the filtered data
    const recentStartDate = new Date(Date.UTC(...metadata.recentPeriod.start.split('-').map(Number)));
    let recentStartIndex = filteredData.findIndex(point => point.x >= recentStartDate);
    if (recentStartIndex === -1) recentStartIndex = filteredData.length - 1;

    const comparisonData = filteredData.slice(0, recentStartIndex);
    const recentData = filteredData.slice(recentStartIndex);

    // Plot data traces
    const comparisonTrace = {
        x: comparisonData.map(point => point.x),
        y: comparisonData.map(point => point.y),
        mode: 'lines+markers',
        name: 'Historical',
        line: { color: 'grey' },
        marker: { color: 'grey' }
    };

    const recentTrace = {
        x: recentData.map(point => point.x),
        y: recentData.map(point => point.y),
        mode: 'lines+markers',
        name: 'Recent',
        line: { color: 'gold' },
        marker: { color: 'gold' }
    };

    // Connector trace between last point of comparison and first of recent
    let connectorTrace = null;
    if (comparisonData.length > 0 && recentData.length > 0) {
        connectorTrace = {
            x: [comparisonData[comparisonData.length - 1].x, recentData[0].x],
            y: [comparisonData[comparisonData.length - 1].y, recentData[0].y],
            mode: 'lines',
            name: '',
            line: { color: 'gold' },
            showlegend: false
        };
    }

    // Create normal range shaded area (Normal Range)
    const sigmaDates = filteredData.map(point => point.x);
    const upperSigmaY = sigmaDates.map(() => comparisonMean + 2 * comparisonStdDev);
    let lowerSigmaY = sigmaDates.map(() => comparisonMean - 2 * comparisonStdDev);
    lowerSigmaY = lowerSigmaY.map(y => y < 0 ? 0 : y);

    const normalRangeTrace = {
        x: sigmaDates,
        y: upperSigmaY,
        fill: 'tonexty',
        fillcolor: 'rgba(128, 128, 128, 0.2)', // Grey shading
        mode: 'none',
        name: 'Normal Range',
        showlegend: true
    };

    const lowerSigmaTrace = {
        x: sigmaDates,
        y: lowerSigmaY,
        mode: 'lines',
        line: { color: 'transparent' },
        showlegend: false
    };

    // Plot data
    const plotData = [lowerSigmaTrace, normalRangeTrace, comparisonTrace, recentTrace];
    if (connectorTrace) {
        plotData.push(connectorTrace);
    }

    // Helper function to format date as "Month" (used for x-axis labels)
    const formatDateToMonth = (dateStr) => {
        const [year, month] = dateStr.split('-').map(Number);
        const dateObj = new Date(Date.UTC(year, month - 1, 1));
        return dateObj.toLocaleString('default', { month: 'long' });
    };

    const formatDateToMonthYear = (dateObj) => {
        return dateObj.toLocaleDateString('default', { month: 'long', year: 'numeric', timeZone: 'UTC' });
    };
    

    // Generate caption using metadata
    const percentDifference = Math.abs((item.difference / item.comparison_mean) * 100).toFixed(1);
    const isIncrease = item.difference > 0;
    const action = isIncrease ? 'increase' : 'drop';

    const comparisonPeriodStart = formatDateToMonth(metadata.comparisonPeriod.start);
    const comparisonPeriodEnd = formatDateToMonth(metadata.comparisonPeriod.end);
    const recentPeriodLabel = metadata.recentPeriod.entireMonth
        ? `${metadata.recentPeriod.monthYear}`
        : `between ${formatDateToMonth(metadata.recentPeriod.start)} and ${formatDateToMonth(metadata.recentPeriod.end)}`;

    const categoryName = item.category || 'incidents';

    const caption = `In ${recentPeriodLabel}, there were ${item.recent_mean.toFixed(0)} ${categoryName} per month,<br> compared to an average of ${comparisonMean} per month over the last year, <BR> a ${Math.abs(percentDifference).toFixed(0)}% ${action}.`;

    const lastPoint = recentData[recentData.length - 1];
    const annotationText = `${formatDateToMonthYear(lastPoint.x)}:<br> ${lastPoint.y} incidents`;
    console.log("lastpoint.x", lastPoint.x);
    const layout = {  
        title: {
            text: chartTitle,
            font: { size: 14 }
        },
        xaxis: {
            tickformat: '%b', // Only display month
        },
        yaxis: { title: '', rangemode: 'tozero' },
        showlegend: true,
        height: 200,
        legend: {
            orientation: "h",
            x: 0.1,
            y: -0.15,
            xanchor: 'left',
            yanchor: 'top',
            font: { size: 10 }
        },
        margin: { t: 20, b: 30, l: 20, r: 5 },
        annotations: [
            {
                text: annotationText,
                x: lastPoint.x,
                y: lastPoint.y,
                arrowhead: 2,
                ax: -75,
                ay: 20,
                bgcolor: 'rgba(255, 255, 0, 0.7)',
                bordercolor: 'gold',
                borderwidth: 1,
            }
        ],
        autosize: true,
        responsive: true
    };

    // Generate a unique ID for the caption
    const uniqueCaptionId = `caption-${chartContainerId}-${Math.random().toString(36).substr(2, 9)}`;

    // Remove interactive effects and hide caption initially
    const config = {
        staticPlot: true,
        responsive: true // Ensures the chart resizes with the window
    };

    // Return full HTML code for rendering the chart
    return `
        <div id="${chartContainerId}" class="chart-container" onmouseover="document.getElementById('${uniqueCaptionId}').style.display = 'block'" onmouseout="document.getElementById('${uniqueCaptionId}').style.display = 'none'"></div>
        <div id="${uniqueCaptionId}" style="display: none; font-size: 12px; text-align: left;">${caption}</div>
        <script>
            (function() {
                const data = ${JSON.stringify(plotData)};
                const layout = ${JSON.stringify(layout)};
                const config = ${JSON.stringify(config)};
                Plotly.newPlot('${chartContainerId}', data, layout, config);
            })();
        </script>
    `;
};
