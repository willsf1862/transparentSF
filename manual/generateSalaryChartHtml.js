import { getEmployeeSalaryQuery } from './queries.mjs';
import { fetchDataFromAPI } from './api.js';

/**
 * Generates the HTML and JavaScript code for a Plotly chart that shows salary history.
 *
 * @param {string} employeeName - The name of the employee.
 * @param {string} chartContainerId - The ID of the HTML element where the chart should be rendered.
 * @returns {Promise<string>} - The HTML and JavaScript code to render the salary chart.
 */
export const generateSalaryChartHtml = async (employeeName, chartContainerId) => {
    try {
        // Fetch salary data from API using the employee name
        const salaryQuery = getEmployeeSalaryQuery(employeeName);
        const salaryDataResult = await fetchDataFromAPI(salaryQuery);

        if (!salaryDataResult || !salaryDataResult.data || salaryDataResult.data.length === 0) {
            throw new Error(`No salary data found for employee: ${employeeName}`);
        }

        const salaryData = salaryDataResult.data;

        // Prepare salary data for Plotly
        const salaryYears = salaryData.map(record => record.year);
        const compensationAmounts = salaryData.map(record => record.total_compensation);

        // Plot data for total compensation history
        const totalCompensationTrace = {
            x: salaryYears,
            y: compensationAmounts,
            type: 'line',
            height: 200,
            name: 'Total Compensation',
            text: compensationAmounts.map(amount => `${Math.round(amount / 1000)}k`),
            textposition: 'inside',
            marker: { color: '#2ca02c' },  // Using a vibrant green color for visibility
        };

        const plotData = [totalCompensationTrace];

        // Layout settings for the Plotly chart - more compact and visible
        const layout = {
            margin: { t: 10, b: 30, l: 20, r: 10 },
            xaxis: { title: '', tickangle: -45 },  // Removed x-axis title for simplicity
            yaxis: { title: '', rangemode: 'tozero', showticklabels: false },  // Removed y-axis title and labels
            showlegend: false,  // Removed legend
        };

        // Remove interactive effects
        const config = {
            staticPlot: true
        };

        // Generate the link to the raw query for transparency
        const queryLink = `<a href="${salaryDataResult.queryURL}" target="_blank">DataSF Link</a>`;

        // Return full HTML code for rendering the chart, including the link to the raw query
        return `
            <div id="${chartContainerId}" class="chart-container"></div>
            <script>
                (function() {
                    const data = ${JSON.stringify(plotData)};
                    const layout = ${JSON.stringify(layout)};
                    const config = ${JSON.stringify(config)};
                    Plotly.newPlot('${chartContainerId}', data, layout, config);
                })();
            </script>
            <div class="caption">
                ${queryLink}
            </div>
        `;
    } catch (error) {
        console.error("Failed to generate salary chart HTML:", error);
        return `<p>Could not load salary history for ${employeeName}.</p>`;
    }
};
