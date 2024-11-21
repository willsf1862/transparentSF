import fs from 'fs';
import ejs from 'ejs';
import puppeteer from 'puppeteer';

// Step 1: Load the Data
const loadData = () => {
  try {
    const data = JSON.parse(fs.readFileSync('data_2018-01-01_to_2024-10-01.json', 'utf8'));
    return data;
  } catch (err) {
    console.error("Error loading data:", err);
    return [];
  }
};

const aggregateData = (data, dimension, filterConditions = []) => {
  const aggregatedData = {};

  // Step 2.1: Filter Data based on `filterConditions`
  const filteredData = data.filter((item) => {
    return filterConditions.every(({ field, operator, value }) => {
      switch (operator) {
        case '==':
          return item[field] === value;
        case '!=':
          return item[field] !== value;
        case '<':
          return parseFloat(item[field]) < parseFloat(value);
        case '<=':
          return parseFloat(item[field]) <= parseFloat(value);
        case '>':
          return parseFloat(item[field]) > parseFloat(value);
        case '>=':
          return parseFloat(item[field]) >= parseFloat(value);
        default:
          return true; // If the operator is unrecognized, don't filter it out
      }
    });
  });

  // Step 2.2: Aggregate Data by Year-Month and the chosen dimension
  filteredData.forEach((item) => {
    const yearMonth = `${item.year}-${String(item.month).padStart(2, '0')}`; // Ensure correct zero-padding
    const key = item[dimension] || 'Unknown';

    if (!aggregatedData[key]) {
      aggregatedData[key] = {};
    }

    if (!aggregatedData[key][yearMonth]) {
      aggregatedData[key][yearMonth] = 0;
    }
    aggregatedData[key][yearMonth] += parseInt(item.count);
  });

  // Step 2.3: Fill in missing months with zero
  const allMonths = getAllYearMonthCombinations(data);
  Object.keys(aggregatedData).forEach((key) => {
    allMonths.forEach((month) => {
      if (!aggregatedData[key][month]) {
        aggregatedData[key][month] = 0;
      }
    });

    // Sort the data for each dimension key by year-month
    aggregatedData[key] = Object.keys(aggregatedData[key])
      .sort((a, b) => new Date(a + '-01') - new Date(b + '-01'))
      .reduce((acc, cur) => {
        acc[cur] = aggregatedData[key][cur];
        return acc;
      }, {});
  });

  return aggregatedData;
};

const getAllYearMonthCombinations = (data) => {
  const uniqueYearMonths = new Set();
  data.forEach((item) => {
    // Ensure year and month are zero-padded to sort correctly
    const yearMonth = `${item.year}-${String(item.month).padStart(2, '0')}`;
    uniqueYearMonths.add(yearMonth);
  });
  return Array.from(uniqueYearMonths).sort((a, b) => new Date(a + '-01') - new Date(b + '-01'));
};

// Step 3: Generate Chart using Plotly and Puppeteer
const generateChart = async (aggregatedData, dimension, title) => {
  // Extract all unique Year-Month labels from the aggregated data
  const yearMonthLabels = Object.keys(aggregatedData[Object.keys(aggregatedData)[0]]);

  // Sort the year-month labels to ensure they are in chronological order
  yearMonthLabels.sort((a, b) => new Date(a + '-01') - new Date(b + '-01'));

  // Convert year-month strings to date strings in ISO format for Plotly
  const xValues = yearMonthLabels.map((label) => {
    const [year, month] = label.split('-');
    return `${year}-${month}-01`;
  });

  // Create traces for each key in the aggregated data
  const traces = Object.keys(aggregatedData).map((key, index) => {
    return {
      x: xValues,
      y: yearMonthLabels.map((yearMonth) => aggregatedData[key][yearMonth] || 0),
      stackgroup: 'one',
      name: key,
      mode: 'lines',
      line: { width: 1 },
    };
  });

  // Layout configuration
  const layout = {
    title: title,
    xaxis: {
      type: 'date',
      title: 'Year-Month',
    },
    yaxis: {
      title: 'Incident Count',
    },
    showlegend: false, // Initially hide the legend
  };

  // Generate an HTML string that includes the Plotly chart
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8" />
      <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
      <div id="chart" style="width:800px;height:600px;"></div>
      <script>
        const data = ${JSON.stringify(traces)};
        const layout = ${JSON.stringify(layout)};
        const config = { responsive: true };
        Plotly.newPlot('chart', data, layout, config);

        // Show legend on hover
        const chartDiv = document.getElementById('chart');
        chartDiv.on('plotly_hover', function() {
          Plotly.relayout(chartDiv, { showlegend: true });
        });
        chartDiv.on('plotly_unhover', function() {
          Plotly.relayout(chartDiv, { showlegend: false });
        });
      </script>
    </body>
    </html>
  `;

  // Use puppeteer to render the HTML and take a screenshot
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  await page.setContent(htmlContent, { waitUntil: 'networkidle0' });
  await page.waitForSelector('#chart'); // Wait for the chart to render

  // Take a screenshot of the chart element
  const chartElement = await page.$('#chart');
  const screenshotBuffer = await chartElement.screenshot();

  await browser.close();

  return screenshotBuffer;
};

// Step 4: Generate HTML Output with Multiple Sections
const generateHtml = async (sections) => {
  const template = `
  <!DOCTYPE html>
  <html lang="en">
  <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Aggregated Incident Report</title>
      <style>
        body { font-family: Arial, sans-serif; }
        header { background-color: #f8f8f8; padding: 20px; text-align: center; }
        header h1 { margin: 0; }
        header .beta { font-size: 0.8em; color: #555; }
        section { margin: 40px; }
        h2 { color: #333; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: center; }
        footer { background-color: #f8f8f8; padding: 10px; text-align: center; margin-top: 40px; }
      </style>
  </head>
  <body>
      <header>
          <h1>Transparent SF <span class="beta">(beta)</span></h1>
      </header>
      <% sections.forEach((section) => { %>
        <section>
          <h2><%= section.title %></h2>
          <img src="<%= section.chartPath %>" alt="Incident Chart">
          <h3>Aggregated Data Table</h3>
          <table>
              <tr>
                  <th>Year-Month</th>
                  <% Object.keys(section.data).forEach((dimension) => { %>
                      <th><%= dimension %></th>
                  <% }); %>
              </tr>
              <% 
                const allMonths = section.data && Object.keys(section.data).length > 0 
                  ? Object.keys(section.data[Object.keys(section.data)[0]])
                  : []; 
                allMonths.forEach((month) => { %>
                  <tr>
                      <td><%= month %></td>
                      <% Object.keys(section.data).forEach((dimension) => { %>
                          <td><%= section.data[dimension][month] %></td>
                      <% }); %>
                  </tr>
              <% }); %>
          </table>
        </section>
      <% }); %>
      <footer>
          <p>&copy; ${new Date().getFullYear()} Transparent SF</p>
      </footer>
  </body>
  </html>
  `;

  const html = await ejs.render(template, { sections });
  fs.writeFileSync('report.html', html);
};

// Main Flow
(async () => {
  // Load data from file
  const data = loadData();

  // Prepare an array to hold sections
  const sections = [];

  // First Chart: With Filter Condition
  {
    // Aggregate data by year and month, with a specified dimension
    const dimension = 'category_group';

    // Define the filter conditions (e.g., filter where `report_type_code == 'II'`)
    const filterConditions = [
      { field: 'report_type_code', operator: '==', value: 'II' },
    ];

    const aggregatedData = aggregateData(data, dimension, filterConditions);

    // Generate chart for the aggregated data
    const chartBuffer = await generateChart(aggregatedData, dimension, 'Incident Reports (Filtered by report_type_code == "II")');
    const chartFilePath = 'chart_filtered.png';
    fs.writeFileSync(chartFilePath, chartBuffer);

    // Add the section data
    sections.push({
      title: 'Incident Reports Filtered by report_type_code == "II"',
      chartPath: chartFilePath,
      data: aggregatedData,
    });
  }

  // Second Chart: Without Filter Condition
  {
    // Aggregate data by year and month, with the same dimension
    const dimension = 'category_group';

    // No filter conditions
    const filterConditions = [];

    const aggregatedData = aggregateData(data, dimension, filterConditions);

    // Generate chart for the aggregated data
    const chartBuffer = await generateChart(aggregatedData, dimension, 'Incident Reports (All Data)');
    const chartFilePath = 'chart_all.png';
    fs.writeFileSync(chartFilePath, chartBuffer);

    // Add the section data
    sections.push({
      title: 'Incident Reports (All Data)',
      chartPath: chartFilePath,
      data: aggregatedData,
    });
  }

  // Generate HTML report with all sections
  await generateHtml(sections);
})();
