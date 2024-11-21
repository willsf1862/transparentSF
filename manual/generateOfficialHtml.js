import { generateAnomalyHtml } from './generateAnomalyHtml.js';
import { generateSalaryChartHtml } from './generateSalaryChartHtml.js';
import { generateCategoryChartHtml } from './generateCategoryChartHtml.js';
import { generateColoredMapHtml } from './generateColoredMapHtml.js';
import { anomalyDetection } from './anomalyDetection.js'; 
import { loadData } from './dataLoader.js';
import { getIncidentComparisonWithFiltersQuery } from './queries.mjs';
import OpenAI from "openai";
import dotenv from 'dotenv';
import { fetchDataFromAPI } from './api.js';

dotenv.config();
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    dangerouslyAllowBrowser: true,
  });

// Securely import your Mapbox access token from environment variables
const mapboxToken = process.env.MAPBOX_ACCESS_TOKEN || 'your-mapbox-access-token-here';
 function processDistrictData(districtData) {
        // Initialize an empty object to store aggregated data
        const aggregatedDistrictData = {};
    
        // Loop through the data to organize it by supervisor_district
        districtData.forEach(entry => {
            const supervisor_district = entry.supervisor_district || "City-wide";
            const year = parseInt(entry.year, 10);
            const count = parseInt(entry.count, 10);
    
            // Initialize the district in the aggregated data if not already done
            if (!aggregatedDistrictData[district]) {
                aggregatedDistrictData[district] = {
                    district_number: district,
                    recent_count: 0,
                    comparison_count: 0
                };
            }
    
            // Update recent_count or comparison_count based on the year
            if (year === 2024) {
                aggregatedDistrictData[district].recent_count = count;
            } else if (year === 2023) {
                aggregatedDistrictData[district].comparison_count = count;
            }
        });
    
        // Now compute the percent change for each district
        Object.keys(aggregatedDistrictData).forEach(district => {
            const data = aggregatedDistrictData[district];
            const { recent_count, comparison_count } = data;
    
            // Calculate percent change only if comparison_count is available and not zero
            if (comparison_count !== 0) {
                data.percent_change = ((recent_count - comparison_count) / comparison_count) * 100;
            } else {
                data.percent_change = null; // No change if we cannot calculate
            }
        });
    
        // Convert the result back to an array format
        return Object.values(aggregatedDistrictData);
    }
export async function generateOfficialHtml({ supervisor, sup_dist_num, metadata }) {
    let districtNum = sup_dist_num === "Mayor" ? sup_dist_num : parseInt(sup_dist_num, 10);
    const title = districtNum === "Mayor" ? "San Francisco Mayor" : `San Francisco Supervisor - District ${districtNum}`;
    const name = districtNum === "Mayor" ? supervisor.employee_identifier : supervisor.sup_name;
 
    function processDistrictData(districtData) {
        // Initialize an empty object to store aggregated data
        const aggregatedDistrictData = {};
    
        // Loop through the data to organize it by supervisor_district
        districtData.forEach(entry => {
            const supervisor_district = entry.supervisor_district || "City-wide";
            const year = parseInt(entry.year, 10);
            const count = parseInt(entry.count, 10);
    
            // Initialize the district in the aggregated data if not already done
            if (!aggregatedDistrictData[supervisor_district]) {
                aggregatedDistrictData[supervisor_district] = {
                    supervisor_district: supervisor_district,
                    recent_count: 0,
                    comparison_count: 0
                };
            }
    
            // Update recent_count or comparison_count based on the year
            if (year === 2024) {
                aggregatedDistrictData[supervisor_district].recent_count = count;
            } else if (year === 2023) {
                aggregatedDistrictData[supervisor_district].comparison_count = count;
            }
        });
    
        // Now compute the percent change for each district
        Object.keys(aggregatedDistrictData).forEach(district => {
            const data = aggregatedDistrictData[district];
            const { recent_count, comparison_count } = data;
    
            // Calculate percent change only if comparison_count is available and not zero
            if (comparison_count !== 0) {
                data.percent_difference = ((recent_count - comparison_count) / comparison_count) ;
            } else {
                data.percent_difference = null; // No change if we cannot calculate
            }
        });
    
        // Convert the result back to an array format
        return Object.values(aggregatedDistrictData);
    }
    // Generate the salary chart HTML
    let salaryChartHtml = "";
    try {
        salaryChartHtml = await generateSalaryChartHtml(name, 'salaryChart');
    } catch (error) {
        salaryChartHtml = `<p>Unable to load salary chart for ${name}.</p>`;
    }
 
    // Generate violent crime chart data, layout, and description
    let violentCrimeChartData = null;
    let violentCrimeLayout = null;
    let violentCrimeDescription = "";
    try {
        const violentCrimeChart = await generateCategoryChartHtml(districtNum, [
            { field: 'report_type_code', operator: '==', value: 'II' },
            { field: 'category_group', operator: '==', value: 'Violent Crime' }
        ]);
        if (violentCrimeChart) {
            violentCrimeChartData = violentCrimeChart.data;
            violentCrimeLayout = {
                ...violentCrimeChart.layout,
                annotations: violentCrimeChart.layout.annotations
            };
            violentCrimeDescription = violentCrimeChart.description || "";
        }
    } catch (error) {
        console.error("Failed to load violent crime chart data", error);
    }
    // Define the filter conditions
       let filterConditions = [
        { field: 'report_type_code', operator: '==', value: 'II' },
        { field: 'incident_category', operator: 'in', value:  ['Assault', 'Homicide', 'Rape', 'Robbery']}
    ];
   // Define the district query
let district_query;

// Check if districtNum is not "Mayor"
if (districtNum !== "Mayor") {
    // Pass districtNum into the query if it's not "Mayor"
    district_query = getIncidentComparisonWithFiltersQuery(
        metadata.calculatedStartDateRecent,
        filterConditions,
        districtNum
    );
} else {
    // If districtNum is "Mayor", omit it from the query
    district_query = getIncidentComparisonWithFiltersQuery(
        metadata.calculatedStartDateRecent,
        filterConditions
    );
}

    const districtData = await fetchDataFromAPI(district_query);
     if (!districtData || !districtData.data || districtData.data.length === 0) {
         throw new Error(`No data found for: ${filterConditions}`);
     }

    const dincidentData = processDistrictData(districtData.data);

    // Generate the colored map HTML with both current and prior month counts for Violent Crime as well. 

    let VCcoloredMapHtml = "";
    try {
        VCcoloredMapHtml = await generateColoredMapHtml(dincidentData, mapboxToken, 'percent_difference','vcmap') 
            ;
    } catch (error) {
        console.error('Error generating map HTML:', error);
        VCcoloredMapHtml = `<p>Unable to load the map.</p>`;
    }

    // Generate protpery versin
    // Define the filter conditions
     filterConditions = [
        { field: 'report_type_code', operator: '==', value: 'II' },
        { field: 'incident_category', operator: 'in', value: ['Burglary', 'Malicious Mischief', 'Embezzlement', 'Larceny Theft', 'Stolen Property', 'Vandalism', 'Motor Vehicle Theft', 'Arson'] }
    ];
   // Define the district query
    let pdistrict_query;

    // Check if districtNum is not "Mayor"
    if (districtNum !== "Mayor") {
        // Pass districtNum into the query if it's not "Mayor"
        pdistrict_query = getIncidentComparisonWithFiltersQuery(
            metadata.calculatedStartDateRecent,
            filterConditions,
            districtNum
        );
    } else {
        // If districtNum is "Mayor", omit it from the query
        pdistrict_query = getIncidentComparisonWithFiltersQuery(
            metadata.calculatedStartDateRecent,
            filterConditions
        );
    }

     let pdistrictData = await fetchDataFromAPI(pdistrict_query);
     if (!pdistrictData || !pdistrictData.data || pdistrictData.data.length === 0) {
         throw new Error(`No data found for: ${filterConditions}`);
     }

     let pdincidentData = processDistrictData(pdistrictData.data);

    // Generate the colored map HTML with both current and prior month counts for Violent Crime as well. 

    let PCcoloredMapHtml = "";
    try {
        PCcoloredMapHtml = await generateColoredMapHtml(pdincidentData, mapboxToken, 'percent_difference','pcmap');
    } catch (error) {
        console.error('Error generating map HTML:', error);
        PCcoloredMapHtml = `<p>Unable to load the map.</p>`;
    }
    // Generate property crime chart data, layout, and description
    let propertyCrimeChartData = null;
    let propertyCrimeLayout = null;
    let propertyCrimeDescription = "";
    try {
        const propertyCrimeChart = await generateCategoryChartHtml(districtNum, [
            { field: 'report_type_code', operator: '==', value: 'II' },
            { field: 'category_group', operator: '==', value: 'Property Crime' }
        ]);
        if (propertyCrimeChart) {
            propertyCrimeChartData = propertyCrimeChart.data;
            propertyCrimeLayout = {
                ...propertyCrimeChart.layout,
                annotations: propertyCrimeChart.layout.annotations
            };
            propertyCrimeDescription = propertyCrimeChart.description || "";
        }
    } catch (error) {
        console.error("Failed to load property crime chart data", error);
    }
    const data = await loadData("data_2018-01-01_to_2024-11-01.json");
    const lastUpdated = metadata ? metadata.last_updated : "N/A";
    

    // Define the filter conditions
    filterConditions = [
        { field: 'report_type_code', operator: '==', value: 'II' },
    ];

    if (typeof districtNum === 'number' && !isNaN(districtNum)) {
        filterConditions.push({ field: 'supervisor_district', operator: '==', value: String(districtNum) });
    }
    
    // Perform anomaly detection for 'incident_category'
    const { results: anomalies, metadata: metadataCategory } = anomalyDetection(data, filterConditions, 'incident_category');
    let anomaliesHtml = anomalies.length > 0 ? await generateAnomalyHtml(anomalies,  metadataCategory, 'Incident Category','CAT') : "<p>No anomalies detected for 'incident_category'.</p>";

    // Perform anomaly detection for 'incident_subcategory'
    const { results: anomaliesSub, metadata: metadataSubCategory } = anomalyDetection(data, filterConditions, 'incident_subcategory');
    let anomaliesSubHtml = anomaliesSub.length > 0 ? await generateAnomalyHtml(anomaliesSub,  metadataSubCategory,' Incident Subcategory','SUBCAT') : "<p>No anomalies detected for 'incident_subcategory'.</p>";
    //console.log ("metadataSubCategory:", metadataSubCategory);
    //console.log ("anomaliesSub:", anomaliesSub);
    // Perform anomaly detection for 'incident_description'
    const { results: anomaliesDesc, metadata: metadataDescCategory } = anomalyDetection(data, filterConditions, 'incident_description');
    let anomaliesDescHtml = anomaliesDesc.length > 0 ? await generateAnomalyHtml(anomaliesDesc, metadataDescCategory,' Incident Description','DESC') : "<p>No anomalies detected for 'incident_description'.</p>";
// **Step 1: Prepare Content for Summarization**
    // Extract the relevant sections you want to summarize.
    // For example, you might want to summarize the crime statistics and anomalies.
    const summaryContent = `
        <h2>${title} - ${name}</h2>
        <h3>Violent Crime Statistics</h3>
        ${violentCrimeDescription}
        <h3>Property Crime Statistics</h3>
        ${propertyCrimeDescription}
        <h3>Anomalies Detected</h3>
        ${anomaliesHtml}
        ${anomaliesSubHtml}
        ${anomaliesDescHtml}
    `;

    // **Step 2: Generate Summary Using OpenAI's ChatGPT**
    let summary = "";
    try {
        const prompt = `
            Provide a concise and accurate summary of the following conetent.  Your summary should be in HTML, but without HTML start and end tags, we are adding it to an existing doc. Start with headline make sure it includes the name of the month we are evaluating.  Below, proivide a two sentence summary of progress and four bullet points of noteworthy insights, each of which should have an emoji as it's first character for the insight.  The emoji should relate to adjectives in the bullet point.  Make them fun and interesting. 
            
            Look at the category changes and subcategory changes for the most explanatory increases or decreases even if they are not out of bounds.  If they are large in absolute number of incidents, they are also of interest to people.  Color decrease words in green and words increase words in red. Dont't use any information outside of these facts.  When possible, inner-link the cagegory name (blue link) to the more detailed charts or tables.  Include the number of incidents when possible in the summary.  Be concise and precise. Don't use value words or express a point of view on why things are changing.  

            ${summaryContent}
        `;

        const response = await openai.chat.completions.create({
            model: "gpt-4o",
            messages: [
                { role: "system", content: "You are a helpful assistant." },
                {
                    role: "user",
                    content: prompt,
                },
            ],
        });

        summary =response.choices[0].message.content.trim();
    } catch (error) {
        console.error("Error generating summary with OpenAI:", error);
        summary = "<p>Unable to generate summary at this time.</p>";
    }

        // **Step 3: Include the Summary in the Final HTML with Collapsible Functionality**
    return `
      <!DOCTYPE html>
      <html lang="en">
      <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>${name} - ${title}</title>
          <link rel="stylesheet" href="style.css">
          <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
          <style>
              /* Add styles for description divs */
              .chart-description {
                  padding: 10px;
                  margin-top: 10px;
                  border-radius: 5px;
                  color: #333;
                  font-size: 14px;
              }
              /* Optional: Adjust text alignment */
              .chart-description p {
                  margin: 0;
              }
              /* Style for the summary section */
              .summary {
                  padding: 15px;
                  background-color: #e8f4f8;
                  border-radius: 5px;
                  margin-bottom: 20px;
              }
          </style>
      </head>
      <body>
          <header>
              <div class="header-title">Transparent SF <span class="beta-label">Beta</span></div>
          </header>
          <main>
              <section>
                  <div class="official-title">${title}</div>
                  <div class="official-name">
                      ${name}
                      <span class="more-text" onclick="toggleSalaryChart()">more...</span>
                      <div id="salaryChartContainer" style="display: none;">
                          <div class="compensation-header">Compensation</div>
                          <div id="salaryChart">${salaryChartHtml}</div>
                      </div>
                  </div>
              </section>

              <!-- **Step 4: Insert the Summary Section Here** -->
              <section class="summary">
                  <p>${summary}</p>
              </section>

                          <!-- Crime Charts -->
              <section class="category-info">
                              <div id="violentCrimeChartContainer" class="chart-container">
                                  <div id="violentCrimeChart"></div>
                                  ${violentCrimeDescription ? `<div class="chart-description" style="background-color: ${violentCrimeLayout && violentCrimeLayout.plot_bgcolor ? violentCrimeLayout.plot_bgcolor : '#f0f0f0'};">${violentCrimeDescription}</div>` : ''}
                              </div>
                              <!-- Map Container -->
                              <section>
                                      ${VCcoloredMapHtml}
                              </section>
                              <div id="propertyCrimeChartContainer" class="chart-container">
                                  <div id="propertyCrimeChart"></div>
                                  ${propertyCrimeDescription ? `<div class="chart-description" style="background-color: ${propertyCrimeLayout && propertyCrimeLayout.plot_bgcolor ? propertyCrimeLayout.plot_bgcolor : '#f0f0f0'};">${propertyCrimeDescription}</div>` : ''}
                              </div>
                              <!-- Map Container -->
                              <section>
                                    ${PCcoloredMapHtml}
                              </section>
              </section>

                          <!-- Anomalies Table -->
                          <section>
                              <h2>Anomalies Detected</h2>
                              <div id="anomaliesContainer">
                                  ${anomaliesHtml}
                              </div>
                              <div id="anomaliesContainer1">
                                  ${anomaliesSubHtml}
                              </div>
                              <div id="anomaliesContainer2">
                                  ${anomaliesDescHtml}
                              </div>
              </section>
          </main>
          <footer>
              <p>Last Updated: ${lastUpdated}</p>
          </footer>

          <script>
              document.addEventListener('DOMContentLoaded', function() {
                  const plotConfig = {
                      responsive: true,
                      displayModeBar: false // Disable the modebar
                  };

                  // Render violent crime chart
                  if (${violentCrimeChartData !== null}) {
                      Plotly.newPlot('violentCrimeChart', ${JSON.stringify(violentCrimeChartData)}, ${JSON.stringify(violentCrimeLayout)}, plotConfig);
                  } else {
                      document.getElementById('violentCrimeChart').innerHTML = '<p>Unable to load violent crime chart for ${name}.</p>';
                  }

                  // Render property crime chart
                  if (${propertyCrimeChartData !== null}) {
                      Plotly.newPlot('propertyCrimeChart', ${JSON.stringify(propertyCrimeChartData)}, ${JSON.stringify(propertyCrimeLayout)}, plotConfig);
                  } else {
                      document.getElementById('propertyCrimeChart').innerHTML = '<p>Unable to load property crime chart for ${name}.</p>';
                  }

                  function toggleSalaryChart() {
                      const chartContainer = document.getElementById('salaryChartContainer');
                      chartContainer.style.display = chartContainer.style.display === 'block' ? 'none' : 'block';
                      if (chartContainer.style.display === 'block') {
                          Plotly.Plots.resize('salaryChart'); // Resize if necessary
                      }
                  }
              });
          </script>
          <script>
              // Define the function globally
              function toggleSalaryChart() {
                  const chartContainer = document.getElementById('salaryChartContainer');
                  chartContainer.style.display = chartContainer.style.display === 'block' ? 'none' : 'block';
                  if (chartContainer.style.display === 'block') {
                      Plotly.Plots.resize('salaryChart'); // Resize if necessary
                  }
              }

            <script>
    // Function to handle collapsible elements
              document.addEventListener('DOMContentLoaded', function() {
                  // Select all collapsible buttons
                  const collapsibleButtons = document.querySelectorAll('.collapsible');
                  
                  // Add click event listeners to each button
                  collapsibleButtons.forEach(button => {
                      button.addEventListener('click', function() {
                          // Toggle the active class on the button
                          this.classList.toggle('active');
                          
                          // Get the next sibling element (the collapsible content)
                          const content = this.nextElementSibling;
                          
                          // Toggle the display of the content
                          if (content.style.display === 'block') {
                              content.style.display = 'none';
                          } else {
                              content.style.display = 'block';
                          }
                      });
                  });

                  // Plotly charts rendering
                  const plotConfig = {
                      responsive: true,
                      displayModeBar: false // Disable the modebar
                  };

                  // Render violent crime chart
                  if (${violentCrimeChartData !== null}) {
                      Plotly.newPlot('violentCrimeChart', ${JSON.stringify(violentCrimeChartData)}, ${JSON.stringify(violentCrimeLayout)}, plotConfig);
                  } else {
                      document.getElementById('violentCrimeChart').innerHTML = '<p>Unable to load violent crime chart for ${name}.</p>';
                  }

                  // Render property crime chart
                  if (${propertyCrimeChartData !== null}) {
                      Plotly.newPlot('propertyCrimeChart', ${JSON.stringify(propertyCrimeChartData)}, ${JSON.stringify(propertyCrimeLayout)}, plotConfig);
                  } else {
                      document.getElementById('propertyCrimeChart').innerHTML = '<p>Unable to load property crime chart for ${name}.</p>';
                  }

                  const resizePlotlyCharts = () => {
                      const chartElements = document.querySelectorAll('.chart-container');
                      chartElements.forEach(chartElement => {
                          Plotly.Plots.resize(chartElement);  // Ensure Plotly charts are resized correctly
                      });
                  };

                  // Call resize on window resize to adjust charts
                  window.addEventListener('resize', resizePlotlyCharts);
                  
                  // Initial call to ensure charts are properly sized on page load
                  resizePlotlyCharts();
              });
          </script>

      </body>
      </html>
    `;
}