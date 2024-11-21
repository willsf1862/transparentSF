import fetch from 'node-fetch';
import { getSupervisorQuery } from './queries.mjs';
import { fetchDataFromAPI } from './api.js';

/**
 * Generates HTML with a colored Mapbox map and a data table displaying all aggregated data.
 *
 * @param {Array} aggregatedData - The aggregated data containing metrics for each district.
 * @param {string} mapboxToken - Your Mapbox access token.
 * @param {string} columnName - The name of the column to visualize (e.g., 'percent_difference').
 * @returns {string} - The generated HTML string.
 */
export async function generateColoredMapHtml(aggregatedData, mapboxToken, columnName, containerId) {
    try {
        // Fetch district multipolygon data from the API
        const supervisorQuery = getSupervisorQuery();
        const SDResult = await fetchDataFromAPI(supervisorQuery);

        // Log the result to inspect the structure
        //console.log("aggregatedData:", aggregatedData);

        // Function to compute fillColor based on aggregated data and the specified column
        function getFillColor(value) {
            const minValue = -1;
            const maxValue = 1;

            // Clamp the value within the range [-1, 1]
            const clampedValue = Math.max(minValue, Math.min(maxValue, value));

            if (clampedValue < 0) {
                // Interpolate between green and white
                const percent = (clampedValue - minValue) / (0 - minValue); // 0 to 1
                const r = Math.floor(255 * percent);
                const g = 255;
                const b = Math.floor(255 * percent);
                return `rgb(${r},${g},${b})`; // From green to white
            } else if (clampedValue > 0) {
                // Interpolate between white and red
                const percent = clampedValue / maxValue; // 0 to 1
                const r = 255;
                const g = Math.floor(255 * (1 - percent));
                const b = Math.floor(255 * (1 - percent));
                return `rgb(${r},${g},${b})`; // From white to red
            } else {
                // When clampedValue is exactly 0, return white
                return `rgb(255,255,255)`;
            }
        }

        // Transform the result from the API into a GeoJSON structure
        const geoJsonData = {
            type: "FeatureCollection",
            features: SDResult.data.map(item => {
                const districtNum = parseInt(item.sup_dist_num, 10);
                console.log("District Number from SDResult:", districtNum);

                // Use the correct property name and ensure data types match
                const aggregatedDistrict = aggregatedData.find(d => parseInt(d.supervisor_district, 10) === districtNum);

                if (aggregatedDistrict) {
                    //console.log(`Aggregated District for District ${districtNum}:`, aggregatedDistrict);
                } else {
                    console.warn(`No aggregated data found for District ${districtNum}`);
                }

                // Ensure that we have an aggregatedDistrict and the column exists
                const value = (aggregatedDistrict && columnName in aggregatedDistrict) ? aggregatedDistrict[columnName] : 0;
                //console.log(`Value of ${columnName} for District ${districtNum}:`, value);

                const fillColor = getFillColor(value);

                let coordinates;
                if (typeof item.multipolygon === 'string') {
                    // If it's a string, parse it as JSON
                    try {
                        coordinates = JSON.parse(item.multipolygon);
                    } catch (parseError) {
                        console.error(`Error parsing multipolygon for district ${districtNum}:`, parseError);
                        coordinates = []; // Assign an empty array or handle as needed
                    }
                } else if (Array.isArray(item.multipolygon)) {
                    // If it's already an array, use it directly
                    coordinates = item.multipolygon;
                } else if (typeof item.multipolygon === 'object') {
                    // If it's an object (but not an array), ensure it's a valid GeoJSON MultiPolygon
                    coordinates = item.multipolygon.coordinates || []; // Adjust based on actual structure
                } else {
                    // Handle unexpected types
                    console.warn(`Unexpected type for multipolygon in district ${districtNum}:`, typeof item.multipolygon);
                    coordinates = []; // Assign an empty array or handle as needed
                }

                const properties = {
                    district: districtNum,
                    supervisor: item.sup_name,
                    fillColor: fillColor,
                    value: value,
                    ...(aggregatedDistrict || {})
                };

                return {
                    type: "Feature",
                    properties: properties,
                    geometry: {
                        type: "MultiPolygon",
                        coordinates: coordinates
                    }
                };
            })
        };

        // Serialize the GeoJSON data
        const geoJsonDataJson = JSON.stringify(geoJsonData);

        // Helper function to capitalize the first letter
        function capitalizeFirstLetter(string) {
            return string.charAt(0).toUpperCase() + string.slice(1);
        }

        // Get all keys from aggregatedData objects
        const keys = Object.keys(aggregatedData[0]);

        // Create table headers dynamically
        const tableHeaders = keys.map(key => `<th>${capitalizeFirstLetter(key)}</th>`).join('');

        // Create table rows dynamically
        const tableRows = aggregatedData.map(row => `
            <tr>
                ${keys.map(key => `<td>${row[key]}</td>`).join('')}
            </tr>
        `).join('');

        // Calculate the sums for the count columns
        const sumColumns = ['recent_count', 'comparison_count']; // Adjust column names as needed
        let totalCounts = {};
        sumColumns.forEach(key => {
            totalCounts[key] = aggregatedData.reduce((sum, row) => sum + (parseFloat(row[key]) || 0), 0);
        });

        // Create table footer with sums
        const tableFooter = `
            <tfoot>
                <tr>
                    ${keys.map(key => {
                        if (sumColumns.includes(key)) {
                            return `<td><strong>${totalCounts[key]}</strong></td>`;
                        } else {
                            return `<td></td>`;
                        }
                    }).join('')}
                </tr>
            </tfoot>
        `;

        const tableHtml = `
            <p id="toggleDetails" style="color: blue; text-decoration: underline; cursor: pointer;">
                District details here.
            </p>
            <div id="detailsContainer" style="display: none; margin-top: 20px;">
                <table border="1" cellspacing="0" cellpadding="5">
                    <thead>
                        <tr>
                            ${tableHeaders}
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                    ${tableFooter}
                </table>
            </div>
            <script>
                document.getElementById('toggleDetails').addEventListener('click', function() {
                    const container = document.getElementById('detailsContainer');
                    if (container.style.display === 'none') {
                        container.style.display = 'block';
                        this.textContent = 'Hide District Details.';
                    } else {
                        container.style.display = 'none';
                        this.textContent = 'District details here.';
                    }
                });

                // Optional: Style the table for better readability
                const table = document.querySelector('#detailsContainer table');
                if (table) {
                    table.style.width = '100%';
                    table.style.borderCollapse = 'collapse';
                }
            </script>
        `;

        // Return the complete HTML code
        return `
        <div id="${containerId}" style="height: 300px;"></div>
        <script src='https://api.mapbox.com/mapbox-gl-js/v2.14.1/mapbox-gl.js'></script>
        <link href='https://api.mapbox.com/mapbox-gl-js/v2.14.1/mapbox-gl.css' rel='stylesheet' />
        <script>
            (function() {
                const mapboxToken = '${mapboxToken}';
                mapboxgl.accessToken = mapboxToken;

                const map = new mapboxgl.Map({
                    container: '${containerId}',
                    style: 'mapbox://styles/mapbox/light-v10',
                    center: [-122.447303, 37.753574],
                    zoom: 11
                });

                // Disable scroll zoom to prevent the map from zooming when scrolling over it
                map.scrollZoom.disable();

                map.on('load', function () {
                    const geoJsonData = ${geoJsonDataJson};

                    // Add the district data as a source
                    map.addSource('districts-${containerId}', {
                        type: 'geojson',
                        data: geoJsonData
                    });

                    // Add layers, using unique IDs
                    map.addLayer({
                        'id': 'districts-fill-${containerId}',
                        'type': 'fill',
                        'source': 'districts-${containerId}',
                        'paint': {
                            'fill-color': ['get', 'fillColor'],
                            'fill-opacity': .5
                        }
                    });

                    map.addLayer({
                        'id': 'districts-outline-${containerId}',
                        'type': 'line',
                        'source': 'districts-${containerId}',
                        'paint': {
                            'line-color': ['get', 'fillColor'],
                            'line-width': .5
                        }
                    });

                    // Add labels showing recent_count, comparison_count, and percent_difference
                    map.addLayer({
                        'id': 'district-labels-${containerId}',
                        'type': 'symbol',
                        'source': 'districts-${containerId}',
                        'layout': {
                            'text-field': [
                                'format',
                                ['concat', 'District ', ['get', 'district']], {}, '\\n',
                                ['concat',
                                    ['get', 'comparison_count'], '->', ['get', 'recent_count'], ' (',
                                    ['to-string', ['round', ['*', ['get', 'percent_difference'], 100]]], '%)'
                                ], {}
                            ],
                            'text-size': 10,
                            'text-allow-overlap': false,
                            'text-anchor': 'center',
                            'text-offset': [0, 0]
                        },
                        'paint': {
                            'text-color': '#999'
                        }
                    });

                    // Optional: Add hover effect to highlight districts
                    map.on('mouseenter', 'districts-fill-${containerId}', function() {
                        map.getCanvas().style.cursor = 'pointer';
                    });

                    map.on('mouseleave', 'districts-fill-${containerId}', function() {
                        map.getCanvas().style.cursor = '';
                    });
                });
            })();
        </script>
        ${tableHtml}
    `;
} catch (error) {
    console.error('Error generating map HTML:', error);
    return `<p>Unable to load the map.</p>`;
}
}