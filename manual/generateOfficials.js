import { anomalyDetection } from './anomalyDetection.js';
import { generateOfficialHtml } from './generateOfficialHtml.js';
import { fetchDataFromAPI } from './api.js';
import { getSupervisorQuery, getCurrentMayorQuery } from './queries.mjs';
import { calculateDates } from './dateUtils.js';
import fs from 'fs';
import path from 'path';

async function generateOfficialsPages() {
  try {
    // Step 1: Load supervisor data from the API
    const supervisorResult = await fetchDataFromAPI(getSupervisorQuery());
    if (!supervisorResult || !supervisorResult.data || supervisorResult.data.length === 0) {
      throw new Error('No supervisor data found.');
    }
    const supervisorData = supervisorResult.data;

    // Step 2: Load mayor data from the API
    const mayorResult = await fetchDataFromAPI(getCurrentMayorQuery());
    if (!mayorResult || !mayorResult.data || mayorResult.data.length === 0) {
      throw new Error('No mayor data found.');
    }
    const mayorData = mayorResult.data[0]; // Get only the most recent Mayor's data.

    // Create output directory if it doesn't exist
    const outputDir = 'output';
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    // Step 3: Calculate metadata values
    const {
      calculatedStartDateRecent,
      calculatedEndDateRecent,
      calculatedStartDateComparison,
      calculatedEndDateComparison,
      updatedOn,
      nextUpdate
    } = calculateDates();

    const metadata = {
      calculatedStartDateRecent,
      calculatedEndDateRecent,
      calculatedStartDateComparison,
      calculatedEndDateComparison,
      updatedOn,
      nextUpdate
    };

    // Step 4: Generate HTML for the Mayor
    const mayorHtmlContent = await generateOfficialHtml({
      supervisor: mayorData,
      sup_dist_num: "Mayor",
      metadata,
    });

    const mayorFileName = `mayor_${mayorData.employee_identifier.replace(/\s+/g, '_').toLowerCase()}.html`;
    const mayorFilePath = path.join(outputDir, mayorFileName);
    await fs.promises.writeFile(mayorFilePath, mayorHtmlContent, 'utf8');
    console.log(`Generated page for Mayor: ${mayorData.employee_identifier}`);

    // Step 5: Generate HTML for each Supervisor
    for (const supervisor of supervisorData) {
      console.log('Metadata for', supervisor.sup_name, ':', metadata); // Verify metadata before generating HTML

      // Generate the HTML content using `generateOfficialHtml`
      const htmlContent = await generateOfficialHtml({
        supervisor,
        sup_dist_num: supervisor.sup_dist_num,
        metadata,
      });

      // Write the HTML content to a file
      const fileName = `${supervisor.sup_name.replace(/\s+/g, '_').toLowerCase()}.html`;
      const filePath = path.join(outputDir, fileName);
      await fs.promises.writeFile(filePath, htmlContent, 'utf8');
      console.log(`Generated page for ${supervisor.sup_name}`);
    }

    // Step 7: Create a master page linking to all officials, including the Mayor
    createMasterPage(supervisorData, mayorData, outputDir);

  } catch (error) {
    console.error('Error generating official pages:', error);
  }
}

// Helper function to generate a master page linking to all officials
// Helper function to generate a master page linking to all officials
function createMasterPage(supervisors, mayor, outputDir) {
  // Create link for Mayor
  const mayorLink = `<li><a href="./mayor_${mayor.employee_identifier.replace(/\s+/g, '_').toLowerCase()}.html">Mayor - ${mayor.employee_identifier}</a></li>`;

  // Create links for Supervisors
  let supervisorLinks = supervisors.map(supervisor => {
    const link = supervisor.sup_name.replace(/\s+/g, '_').toLowerCase();
    return `<li><a href="./${link}.html">District ${parseInt(supervisor.sup_dist_num, 10)} Supervisor - ${supervisor.sup_name}</a></li>`;
  }).join('');

  // Generate the HTML for the master page, similar to the original version
  const masterHtml = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>All Officials - Transparent SF</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f9f9f9;
                line-height: 1.6;
                color: #333;
            }
            header {
                background-color: #f8f8f8;
                padding: 20px;
                text-align: center;
                border-bottom: 1px solid #ddd;
            }
            h1 {
                font-size: 2em;
            }
            section {
                margin-top: 20px;
            }
            h2 {
                font-size: 1.5em;
                color: #444;
            }
            ul {
                list-style-type: none;
                padding-left: 0;
            }
            li {
                margin: 10px 0;
            }
            a {
                text-decoration: none;
                color: #0066cc;
            }
            a:hover {
                text-decoration: underline;
            }
        </style>
    </head>
    <body>
        <header>
            <h1 class="header-title">Transparent SF<span class="beta-label"> Beta</span></h1>
            <p class="sub-title">Accountability through open data</p>
        </header>
        <main>
            <section>
                <h2>Mayor</h2>
                <ul>
                    ${mayorLink}
                </ul>
            </section>
            <section>
                <h2>City Supervisors</h2>
                <ul>
                    ${supervisorLinks}
                </ul>
            </section>
        </main>
    </body>
    </html>
  `;

  const masterFilePath = path.join(outputDir, 'index.html');
  fs.writeFileSync(masterFilePath, masterHtml, 'utf8');
  console.log('Generated master page linking to all officials.');
}


// Run the generation function
generateOfficialsPages();
