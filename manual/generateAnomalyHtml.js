import fs from 'fs';
import ejs from 'ejs';
import { generateChartHtml } from './chartGenerator.js';  // Import the shared function

/**
 * Generates an HTML section for anomalies using EJS templates.
 *
 * @param {Array} anomalies - Array of anomaly objects.
 * @param {Object} metadata - Metadata object containing periods and filter conditions.
 * @param {string} titleField - The title field for the collapsible section.
 * @param {string} windowID - The unique ID for the window where this HTML is displayed.
 * @returns {string} - The HTML content for the report section.
 */
export const generateAnomalyHtml = async (anomalies, metadata, titleField, windowID) => {
    if (!anomalies || anomalies.length === 0) {
        console.error('No anomalies to report.');
        return '';
    }

    // Helper function to format month and year as "Month YYYY"
    const formatMonthYear = (dateStr) => {
        const [year, month, day] = dateStr.split('-').map(Number);
        const monthNames = ["January", "February", "March", "April", "May", "June",
                            "July", "August", "September", "October", "November", "December"];
        return `${monthNames[month - 1]} ${year}`;
    };

    // Determine if the recent period is the entire month
    const isEntireMonth = (period) => {
        const [startYear, startMonth, startDay] = period.start.split('-').map(Number);
        const [endYear, endMonth, endDay] = period.end.split('-').map(Number);
        const lastDay = new Date(startYear, startMonth, 0).getDate();
        return startDay === 1 && endDay === lastDay && startMonth === endMonth && startYear === endYear;
    };
    const recentPeriodEntireMonth = isEntireMonth(metadata.recentPeriod);

    // Format dates in metadata
    const formattedMetadata = {
        ...metadata,
        recentPeriod: {
            ...metadata.recentPeriod,
            entireMonth: recentPeriodEntireMonth,
            monthYear: recentPeriodEntireMonth ? formatMonthYear(metadata.recentPeriod.end) : null
        }
    };

    // Determine the groupField dynamically
    const groupField = Object.keys(anomalies[0]).find(key => ![
        'comparison_mean', 'comparison_stdDev', 'recent_mean', 'difference', 
        'out_of_bounds', 'dates', 'counts', 'comparisonPeriod', 'recentPeriod'
    ].includes(key));

    if (!groupField) {
        console.error('No valid group field found in anomalies data.');
        return '';
    }

    // Define a human-readable label for the group field
    const groupFieldLabel = groupField.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase());

    // Sort anomalies: out_of_bounds first
    const sortedAnomalies = anomalies.sort((a, b) => {
        if (a.out_of_bounds === b.out_of_bounds) return 0;
        return a.out_of_bounds ? -1 : 1;
    });

    // Extract report month and year from recentPeriod.end using the new formatMonthYear function
    const reportMonthYear = formatMonthYear(metadata.recentPeriod.end);

    // Define the report title
    const reportTitle = `${reportMonthYear} Police Incident Category Changes`;

    // Prepare chart HTML using generateChartHtml
    const chartHtmlList = sortedAnomalies
        .filter(item => item.out_of_bounds)
        .map((item, index) => {
            const percentChange = (item.difference / item.comparison_mean) * 100;
            const chartTitle = `<b>${item[groupField]}</b><BR> ${reportMonthYear}${percentChange > 0 ? '+' : '-'}${Math.abs(percentChange).toFixed(0)}% vs average`;
            const chartContainerId = `${windowID}-anomalyChart-${index}`; // Unique ID using windowID
            return generateChartHtml(item, chartTitle, chartContainerId, formattedMetadata);
        });

    // EJS Template for a single report section
    const template = `
<section id="<%= windowID %>">
    <h2><%= reportTitle %></h2>

    <!-- Container for two-column layout -->
    <div class="chart-grid-container">
        <% chartHtmlList.forEach((chartHtml, index) => { %>
            <div class="chart-grid-item">
                <%- chartHtml %>
            </div>
        <% }); %>
    </div>

    <button type="button" class="collapsible" id="collapsible-<%= windowID %>">Details by <%= titleField %></button>
    <div class="content" id="content-<%= windowID %>">
        <table>
            <thead>
                <tr>
                    <th><%= groupFieldLabel %></th>
                    <th>Comparison Mean</th>
                    <th>Comparison Std Dev</th>
                    <th>Recent Mean</th>
                    <th>Difference</th>
                    <th>% Difference</th>
                    <th>Anomaly Detected</th>
                </tr>
            </thead>
            <tbody>
                <% data.forEach((item, index) => { 
                    const percentDifference = ((item.difference / item.comparison_mean) * 100);
                    const isIncrease = percentDifference > 0;
                    const absPercentDifference = Math.abs(Math.round(percentDifference));
                    const color = item.out_of_bounds ? 'rgba(255, 0, 0, 0.1)' : '';
                %>
                    <tr style="background-color: <%= color %>;">
                        <% if (item.out_of_bounds) { %>
                            <td><a href="#<%= windowID %>-anomalyChart-<%= index %>" style="color: #d9534f; text-decoration: none;"><%= item[groupField] %></a></td>
                        <% } else { %>
                            <td><%= item[groupField] %></td>
                        <% } %>
                        <td><%= item.comparison_mean.toFixed(1) %></td>
                        <td><%= item.comparison_stdDev.toFixed(1) %></td>
                        <td><%= item.recent_mean.toFixed(1) %></td>
                        <td><%= item.difference.toFixed(1) %></td>
                        <td><%= absPercentDifference %>%</td>
                        <td><%= item.out_of_bounds ? 'Yes' : 'No' %></td>
                    </tr>
                <% }); %>
            </tbody>
        </table>
    </div>
</section>
<script>
    // Toggle the collapsible content
    document.getElementById('collapsible-<%= windowID %>').addEventListener('click', function() {
        this.classList.toggle('active');
        const content = document.getElementById('content-<%= windowID %>');
        if (content.style.display === 'block') {
            content.style.display = 'none';
        } else {
            content.style.display = 'block';
        }
    });
</script>
`;

    // Render the template with data
    const htmlContent = ejs.render(template, {
        data: sortedAnomalies,
        groupField: groupField,
        groupFieldLabel: groupFieldLabel,
        reportTitle: reportTitle,
        metadata: formattedMetadata,
        chartHtmlList: chartHtmlList,
        titleField: titleField,
        windowID: windowID // Use windowID for unique identification
    });

    // Return the HTML content
    return htmlContent;
};
