import { subDays, endOfMonth } from 'date-fns';
import { loadData } from './dataLoader.js'; // Ensure this path is correct
import { calculateDates } from './dateUtils.js'; // Ensure this path is correct

/**
 * Generate a category chart layout for Plotly.
 * @param {string | number} districtNum - The district number or "Mayor".
 * @param {Array} filterConditions - Array of filter conditions to apply to the data.
 * @returns {Object} - An object containing data, layout, startYear, endYear, and description.
 */
export async function generateCategoryChartHtml(districtNum, filterConditions) {
    try {
        // Load necessary data
        const data = await loadData("data_2018-01-01_to_2024-11-01.json");
        const cityTenuresData = await loadData("city_tenures.json");
        const { 
            calculatedStartDateRecent,
            calculatedEndDateRecent,
            updatedOn,
            nextUpdate 
        } = calculateDates();

        // Prepare and filter data
        const filteredData = prepareData(data, filterConditions, districtNum, calculatedEndDateRecent);
        
        // Aggregate data by month and year
        const { months, counts, startYear, endYear } = aggregateData(filteredData, calculatedEndDateRecent);

        const lastIndex = counts.length - 1;

        // Calculate the x-axis range
        const rangeStart = subDays(months[0], 30); // 30 days before the first month
        const rangeEnd = endOfMonth(months[lastIndex]); // End of the last month

        // Find the index for the same month last year
        const lastDate = months[lastIndex];
        const oneYearPriorDate = new Date(lastDate);
        oneYearPriorDate.setFullYear(lastDate.getFullYear() - 1);

        const oneYearPriorIndex = months.findIndex(date => 
            date.getFullYear() === oneYearPriorDate.getFullYear() && 
            date.getMonth() === oneYearPriorDate.getMonth()
        );

        // Calculate fixed averages
        const average12 = computeFixedAverage(counts, lastIndex, 12);
        const average60 = computeFixedAverage(counts, lastIndex, 60);

        // Generate chart data
        const chartData = generateChartData(months, counts, lastIndex, oneYearPriorIndex, average12, average60);

        // Generate supervisor shades and annotations
        const { supervisorShades, supervisorAnnotations } = generateSupervisorAnnotations(
            cityTenuresData,
            districtNum,
            calculatedEndDateRecent,
            rangeStart,
            months,
            lastIndex
        );

        // Set the chart title and layout
        const categoryGroup = getCategoryGroup(filterConditions);
        const { description, backgroundColor, totalPercentageChange } = generateDescription(months, counts, lastIndex, districtNum, categoryGroup, average12, average60);
        
        const { mainTitle, subtitle } = getChartTitles(
            districtNum, 
            categoryGroup, 
            startYear, 
            endYear, 
            totalPercentageChange, 
            months[lastIndex], 
            oneYearPriorIndex !== -1 ? months[oneYearPriorIndex] : null
        );

        const layout = getChartLayout(
            mainTitle, 
            subtitle, 
            months, 
            counts, 
            lastIndex, 
            oneYearPriorIndex, 
            rangeStart, 
            rangeEnd, 
            supervisorShades, 
            supervisorAnnotations, 
            filterConditions, 
            backgroundColor,
            average12,
            average60
        );

        return {
            data: chartData,
            layout: layout,
            startYear,
            endYear,
            description
        };
    } catch (error) {
        console.error('Error generating category chart HTML:', error);
        return null;
    }   
}

/**
 * Prepare and filter the data based on conditions.
 */
function prepareData(data, filterConditions, districtNum, calculatedEndDateRecent) {
    // Sort the data by year and month
    data.sort((a, b) => {
        if (a.year === b.year) {
            return a.month - b.month;
        }
        return a.year - b.year;
    });

    // Filter data based on conditions
    let filteredData = data.filter(record => {
        return filterConditions.every(condition => {
            switch (condition.operator) {
                case '==':
                    return record[condition.field] == condition.value;
                case '!=':
                    return record[condition.field] != condition.value;
                default:
                    return true;
            }
        });
    });

    // Filter data based on date
    filteredData = filteredData.filter(record => {
        const recordYear = parseInt(record.year, 10);
        const recordMonth = parseInt(record.month, 10);
        const endYear = calculatedEndDateRecent.getFullYear();
        const endMonth = calculatedEndDateRecent.getMonth() + 1;
        return (
            recordYear < endYear ||
            (recordYear === endYear && recordMonth <= endMonth)
        );
    });

    // Filter data based on district
    if (districtNum !== "Mayor") {
        filteredData = filteredData.filter(record => record.supervisor_district == districtNum);
    }

    return filteredData;
}

/**
 * Aggregate data by month and year.
 */
function aggregateData(filteredData, calculatedEndDateRecent) {
    const monthlyCounts = {};
    filteredData.forEach(record => {
        const yearMonth = `${record.year}-${String(record.month).padStart(2, '0')}`;
        if (!monthlyCounts[yearMonth]) {
            monthlyCounts[yearMonth] = 0;
        }
        monthlyCounts[yearMonth] += parseInt(record.count, 10);
    });

    const sortedKeys = Object.keys(monthlyCounts).sort();

    const months = sortedKeys.map(key => {
        const [year, month] = key.split('-').map(Number);
        return new Date(year, month - 1, 1); // Months are zero-based in JavaScript Date
    });

    const counts = sortedKeys.map(key => monthlyCounts[key]);

    const startYear = months[0].getFullYear();
    const endYear = calculatedEndDateRecent.getFullYear();

    return { months, counts, startYear, endYear };
}

/**
 * Compute fixed average over a specified number of months before the lastIndex.
 * @param {Array} counts - Array of incident counts per month.
 * @param {number} lastIndex - Index of the latest month.
 * @param {number} numMonths - Number of months to include in the average.
 * @returns {number|null} - The calculated average or null if not enough data.
 */
function computeFixedAverage(counts, lastIndex, numMonths) {
    const start = lastIndex - numMonths;
    const end = lastIndex - 1; // Exclude the current month
    if (start < 0) {
        return null; // Not enough data to compute the average
    }
    const subset = counts.slice(start, end + 1);
    const sum = subset.reduce((acc, val) => acc + val, 0);
    return Math.round(sum / subset.length);
}

/**
 * Generate chart data for Plotly.
 */
function generateChartData(months, counts, lastIndex, oneYearPriorIndex, average12, average60) {
    const chartData = [
        {
            x: months,
            y: counts,
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Incidents',
            line: { color: '#17BECF' },
            marker: {
                size: 6,
                color: '#17BECF',
            },
            hoverinfo: 'none' // Disable hover info for the main trace
        },
        {
            x: [months[lastIndex]],
            y: [counts[lastIndex]],
            type: 'scatter',
            mode: 'markers',
            name: 'Current Month',
            marker: {
                size: 12,
                color: 'gold',
                symbol: 'circle-open',
                line: {
                    width: 2,
                    color: 'gold'
                },
                ay:-40,
                ax:-40
            },
            showlegend: false,
            hoverinfo: 'skip' // Prevent hover labels
        }
    ];

    // Highlight the data point one year prior to the current month
    if (oneYearPriorIndex !== -1) {
        chartData.push({
            x: [months[oneYearPriorIndex]],
            y: [counts[oneYearPriorIndex]],
            type: 'scatter',
            mode: 'markers',
            name: 'Same Month Last Year',
            marker: {
                size: 12,
                color: 'lightblue',
                symbol: 'circle-open',
                line: {
                    width: 2,
                    color: 'lightblue'
                },
                ax:-40,
                ay:-40,
            },
            hoverinfo: 'skip', // Prevent hover labels,
            showlegend: false
        });
    }

    // Add 12-Month Fixed Average Line
    if (average12 !== null) {
        chartData.push({
            x: [months[0], months[lastIndex]],
            y: [average12, average12],
            type: 'scatter',
            mode: 'lines',
            name: '1yr Avg',
            line: {
                color: 'orange',
                width: 0,
                dash: 'none',
                opacity: 0.5
            },
            hoverinfo: 'none',
            showlegend: true
        });
    }

    // Add 60-Month Fixed Average Line
    if (average60 !== null) {
        chartData.push({
            x: [months[0], months[lastIndex]],
            y: [average60, average60],
            type: 'scatter',
            mode: 'lines',
            name: '5yr Avg',
            line: {
                color: 'purple',
                width: 0,
                dash: 'none',
                opacity: 0.5

            },
            hoverinfo: 'none',
            showlegend: true
        });
    }

    return chartData;
}

/**
 * Generate supervisor tenure annotations and shaded areas.
 */
function generateSupervisorAnnotations(cityTenuresData, districtNum, calculatedEndDateRecent, rangeStart, months, lastIndex) {
    const supervisorShades = [];
    const supervisorAnnotations = [];
    const districtTenures = cityTenuresData.filter(record => record.district == districtNum || (districtNum === "Mayor" && record.district == 0));
    const colors = [
        'rgba(0, 102, 204, 0.4)',  // Deep Blue, medium opacity
        'rgba(51, 153, 255, 0.35)', // Bright Blue, slightly lighter
        'rgba(102, 178, 255, 0.3)', // Lighter Blue, lower opacity
        'rgba(153, 204, 255, 0.25)', // Soft Blue, lower opacity
        'rgba(179, 217, 255, 0.2)', // Light Pastel Blue, very light
        'rgba(204, 229, 255, 0.15)', // Very Pale Blue, very light and low opacity
    ];
    
    for (let i = 0; i < districtTenures.length; i++) {
        const currentTenure = districtTenures[i];
        const nextTenure = districtTenures[i + 1];
        const startDate = new Date(currentTenure.date);
        const startDateFirstOfMonth = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
        const endDate = nextTenure ? new Date(nextTenure.date) : calculatedEndDateRecent;
        const endDateFirstOfMonth = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
        const correctedEndDate = (i === districtTenures.length - 1) 
            ? endOfMonth(calculatedEndDateRecent) 
            : endDateFirstOfMonth;

        if (correctedEndDate <= startDateFirstOfMonth) continue;

        // Adjust shading to not start before the x-axis range
        const adjustedX0 = startDateFirstOfMonth < rangeStart ? rangeStart : startDateFirstOfMonth;

        // Add shaded rectangle
        supervisorShades.push({
            type: 'rect',
            xref: 'x',
            yref: 'paper',
            x0: adjustedX0,
            x1: correctedEndDate,
            y0: 0,
            y1: 1,
            fillcolor: colors[i % colors.length],
            opacity: 0.4,
            line: {
                width: 0,
            }
        });

        // Add supervisor name annotation within the shaded area
        const supervisorName = currentTenure.supervisor || 'Unknown Supervisor';
        supervisorAnnotations.push({
            xref: 'x',
            yref: 'paper',
            x: new Date((adjustedX0.getTime() + correctedEndDate.getTime()) / 2), // Midpoint
            y: 0.96, // Near top
            text: supervisorName,
            showarrow: false,
            font: {
                size: 12,
                color: '#000'
            },
            align: 'center',
            xanchor: 'center',
            yanchor: 'top',
            bgcolor: 'rgba(255, 255, 255, 0.6)',
            bordercolor: '#000',
            borderwidth: 0,
            borderpad: 0
        });
    }

    return { supervisorShades, supervisorAnnotations };
}

/**
 * Get the category group from filter conditions.
 */
function getCategoryGroup(filterConditions) {
    const categoryCondition = filterConditions.find(condition => condition.field === 'category_group');
    return categoryCondition ? categoryCondition.value : 'All';
}

/**
 * Generate chart titles.
 */
function getChartTitles(districtNum, categoryGroup, startYear, endYear, totalPercentageChange, recentDate, sameMonthLastYearDate) {
    // Determine direction based on totalPercentageChange
    const direction = totalPercentageChange > 0 ? 'Up' : (totalPercentageChange < 0 ? 'Down' : 'Stable');
    const percentage = Math.abs(totalPercentageChange).toFixed(0) + '%';

    // Construct main title
    const locationTitle = districtNum === "Mayor" ? 'San Francisco' : `SF District ${districtNum}`;
    const mainTitle = `${locationTitle} ${categoryGroup} ${direction} ${percentage}`;

    // Format dates for subtitle
    const recentMonthYear = recentDate ? `${recentDate.toLocaleString('default', { month: 'long' })} ${recentDate.getFullYear()}` : 'Recent Period';
    const sameMonthLastYear = sameMonthLastYearDate ? `${sameMonthLastYearDate.toLocaleString('default', { month: 'long' })} ${sameMonthLastYearDate.getFullYear()}` : 'Same Month Last Year';

    // Construct subtitle
    const subtitle = `${recentMonthYear} vs ${sameMonthLastYear}`;

    return { mainTitle, subtitle };
}

/**
 * Get the layout configuration for Plotly chart.
 */
function getChartLayout(mainTitle, subtitle, months, counts, lastIndex, oneYearPriorIndex, rangeStart, rangeEnd, supervisorShades, supervisorAnnotations, filterConditions, backgroundColor, average12, average60) {
    const annotations = [
        ...supervisorAnnotations,
        // Annotation for the last data point (current month)
        {
            xref: 'x',
            yref: 'y',
            x: months[lastIndex],
            y: counts[lastIndex],
            text: `${months[lastIndex].toLocaleString('default', { month: 'long', year: 'numeric' })}:<br>${counts[lastIndex]} incidents`,
            showarrow: true,
            arrowhead: 1,
            arrowsize: 1,
            arrowwidth: 1,
            ax: -40, // 40 pixels to the left
            ay: 50, // 40 pixels downward
            font: {
                size: 12,
                color: '#333'
            },
            bgcolor: 'rgba(255, 255, 0, 0.7)',
            bordercolor: 'gold',
            borderwidth: 1,
        },
        // Annotation for the same month last year
        oneYearPriorIndex !== -1 ? {
            xref: 'x',
            yref: 'y',
            x: months[oneYearPriorIndex],
            y: counts[oneYearPriorIndex],
            text: `${months[oneYearPriorIndex].toLocaleString('default', { month: 'long', year: 'numeric' })}:<br>${counts[oneYearPriorIndex]} incidents`,
            showarrow: true,
            arrowhead: 1,
            arrowsize: 1,
            arrowwidth: 1,
            ax: -60, // 40 pixels to the right
            ay: 50, // 40 pixels downward
            font: {
                size: 12,
                color: '#333'
            },
            bgcolor: 'rgba(173, 216, 230, 0.7)', // Light blue background
            bordercolor: 'lightblue',
            borderwidth: 1,
        } : null,
        // Subtitle Annotation
        {
            xref: 'paper',
            yref: 'paper',
            x: 0.5,
            y: 1.2,
            xanchor: 'center',
            yanchor: 'top',
            text: `<span style="font-size: 14px; color: #333;">${subtitle}</span>`,
            showarrow: false,
            align: 'center',
        },
        // Footer Annotations
        {
            xref: 'paper',
            yref: 'paper',
            x: 0,
            y: -0.3,
            xanchor: 'left',
            yanchor: 'top',
            text: `Data Source: <a href="https://data.sfgov.org/Public-Safety/Map-of-Police-Department-Incident-Reports-2018-to-/jq29-s5wp" target="_blank">SF Police Incident Dataset</a>`,
            showarrow: false,
            font: {
                size: 11,
                color: '#AAA'
            },
            align: 'left',
        },
        {
            xref: 'paper',
            yref: 'paper',
            x: 0,
            y: -0.4,
            xanchor: 'left',
            yanchor: 'top',
            text: `Filters Applied: ${filterConditions.map(condition => `${condition.field}: ${condition.value}`).join(' ')}`,
            showarrow: false,
            font: {
                size: 11,
                color: '#aaa'
            },
            align: 'left',
        }
    ].filter(annotation => annotation !== null); // Remove null entries

    return {
        title: {
            text: `<b>${mainTitle}</b>`,
            font: {
                size: 16,
                color: '#333'
            },
            y: 0.95,
            x: 0.5,
            xanchor: 'center',
            yanchor: 'top'
        },
        showlegend: false, // Enable legend to display average lines
        legend: {
            orientation: "h",
            x: 0.1,
            y: -0.4,
            xanchor: 'left',
            yanchor: 'top',
            font: { size: 10 }
        },
        autosize: true,
        height: 300,
        xaxis: {
            type: 'date',
            tickmode: 'linear',
            tick0: months[0].toISOString().split('T')[0],
            dtick: 'M12',
            tickformat: '%Y',
            ticks: 'outside',
            tickwidth: 1,
            ticklen: 10,
            tickcolor: '#000',
            showline: false,
            linewidth: 0,
            mirror: false,
            tickangle: 0,
            showgrid: true,
            gridcolor: '#f0f0f0',
            tickfont: {
                size: 14,
                color: '#333'
            },
            minor: {
                showgrid: true,
                gridwidth: 0.5,
                ticklen: 3,
                tickwidth: 1,
                tickcolor: '#ccc'
            },
            range: [rangeStart, rangeEnd],
        },
        yaxis: {
            tickformat: ',.0f',
            showline: false,
            linewidth: 0,
            mirror: false,
            tickangle: 0,
            showgrid: true,
            gridcolor: '#f0f0f0',
            tickfont: {
                size: 13,
                color: '#333'
            },
            minor: {
                showgrid: true,
                gridwidth: 0.5,
                ticklen: 3,
                tickwidth: 1,
                tickcolor: '#ccc'
            },  
            rangemode: 'tozero' 
        },
        shapes: supervisorShades,
        annotations: annotations,
        margin: {
            l: 38,
            r: 20,
            t: 90,
            b: 60
        },
        paper_bgcolor: backgroundColor,
        plot_bgcolor: 'rgba(255, 255, 255, 0.9)'
    };
}

/**
 * Generate a human-readable description.
 */
function generateDescription(months, counts, lastIndex, districtNum, categoryGroup, average12, average60) {
    const currentMonthDate = months[lastIndex];
    const currentMonthCount = counts[lastIndex];
    const monthName = currentMonthDate.toLocaleString('default', { month: 'long' });
    const location = districtNum === "Mayor" ? 'San Francisco' : `San Francisco District ${districtNum}`;

    // Initialize variables for percentage changes
    let percentageChange1 = 0;
    let percentageChange2 = 0;
    let percentageChange3 = 0;

    // Get same month last year
    const sameMonthLastYearDate = new Date(currentMonthDate);
    sameMonthLastYearDate.setFullYear(currentMonthDate.getFullYear() - 1);

    const sameMonthLastYearIndex = months.findIndex(date => 
        date.getFullYear() === sameMonthLastYearDate.getFullYear() && 
        date.getMonth() === sameMonthLastYearDate.getMonth()
    );

    let sameMonthLastYearCount = null;
    if (sameMonthLastYearIndex !== -1) {
        sameMonthLastYearCount = counts[sameMonthLastYearIndex];
        percentageChange1 = ((currentMonthCount - sameMonthLastYearCount) / sameMonthLastYearCount) * 100;
    }

    // Compute fixed 12-month average
    // const average12 = computeFixedAverage(counts, lastIndex, 12); // Already passed as parameter
    // Compute fixed 60-month average
    // const average60 = computeFixedAverage(counts, lastIndex, 60); // Already passed as parameter

    // Calculate percentage changes relative to fixed averages
    if (average12 !== null) {
        percentageChange2 = ((currentMonthCount - average12) / average12) * 100;
    }

    if (average60 !== null) {
        percentageChange3 = ((currentMonthCount - average60) / average60) * 100;
    }

    // Sum the percentage changes (for title direction)
    const totalPercentageChange = Math.round(percentageChange1);

    // Generate the background color
    const backgroundColor = getBackgroundColor(totalPercentageChange);

    // Construct the description
    let descriptionParts = [];

    descriptionParts.push(`<p>In ${monthName}, ${location} had ${currentMonthCount} incidents of ${categoryGroup}.`);

    if (sameMonthLastYearCount !== null) {
        const adjective = getAdjective(percentageChange1);
        descriptionParts.push(` That's ${adjective} last ${monthName}'s total of ${sameMonthLastYearCount}.`);
    }

    if (average12 !== null) {
        const adjective12Month = getAdjective(percentageChange2);
        descriptionParts.push(` It is also ${adjective12Month} the 1-year average of ${average12} incidents per month,`);
    }

    if (average60 !== null) {
        const adjective60Month = getAdjective(percentageChange3);
        descriptionParts.push(` and it's ${adjective60Month} the 5-year average of ${average60} incidents per month.`);
    }

    descriptionParts.push(`</p>`);

    // Return description, backgroundColor, and totalPercentageChange for title use
    return {
        description: descriptionParts.join(''),
        backgroundColor,
        totalPercentageChange
    };
}

/**
 * Get an adjective based on the percentage change.
 */
function getAdjective(percentageChange) {
    const absDiff = Math.abs(percentageChange);
    if (absDiff > 30) {
        return percentageChange > 0 ? 'substantially higher than' : 'substantially lower than';
    } else if (absDiff > 10) {
        return percentageChange > 0 ? 'higher than' : 'lower than';
    } else if (absDiff > 5) {
        return percentageChange > 0 ? 'slightly higher than' : 'slightly lower than';
    } else {
        return 'about the same as';
    }
}

/**
 * Get a background color based on the percentage change.
 */
function getBackgroundColor(diff) {
    const maxDiff = 150;
    const absDiff = Math.abs(diff);
    const percentage = Math.min(absDiff / maxDiff, 1);

    let hue;
    if (diff < 0) {
        hue = 120; // Green hue
    } else if (diff > 0) {
        hue = 0; // Red hue
    } else {
        return 'hsl(0, 0%, 100%)'; // White color for zero difference
    }

    const lightness = 100 - (percentage * 25);

    return `hsl(${hue}, 100%, ${lightness}%)`;
}
