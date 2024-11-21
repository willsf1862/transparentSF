// main.js
import { loadData } from './dataLoader.js';
import { anomalyDetection } from './anomalyDetection.js';
import { generateAnomalyHtml } from './generateAnomalyHtml.js';
import { generateGhostPost } from './generateGhostPost.js';
import dotenv from 'dotenv';
dotenv.config();
const ghostConfig = {
  url: process.env.GHOST_URL, // e.g., 'https://your-ghost-site.com'
  adminApiKey: process.env.GHOST_ADMIN_API_KEY // e.g., 'YOUR_ADMIN_API_KEY'
};

(async () => {
  // Load data from file
  const data = loadData("data_2018-01-01_to_2024-10-01.json");

  // Define the filter conditions (e.g., filter where `report_type_code == 'II'`)
  let filterConditions = [
    { field: 'report_type_code', operator: '==', value: 'II' },

  ];

// Perform anomaly detection for 'incident_category'
const { results: anomalies, metadata: metadataCategory } = anomalyDetection(data, filterConditions, 'incident_category');
if (anomalies.length > 0) {
  await generateAnomalyHtml(anomalies, metadataCategory, 'output/anomaly_report.html');
} else {
  console.warn("No anomalies detected for 'incident_category'.");
}
//generateGhostPost(anomalies, metadataCategory, ghostConfig);
console.log(anomalies);

// Perform anomaly detection for 'incident_subcategory'
const { results: anomaliesSub, metadata: metadataSubcategory } = anomalyDetection(data, filterConditions, 'incident_subcategory');
if (anomaliesSub.length > 0) {
  await generateAnomalyHtml(anomaliesSub, metadataSubcategory, 'output/anomaly_report_sub.html');
} else {
  console.warn("No anomalies detected for 'incident_subcategory'.");
}
//generateGhostPost(anomaliesSub, metadataSubcategory, ghostConfig);

// Perform anomaly detection for 'incident_description'
const { results: anomaliesDesc, metadata: metadataDescription } = anomalyDetection(data, filterConditions, 'incident_description');
if (anomaliesDesc.length > 0) {
  await generateAnomalyHtml(anomaliesDesc,metadataDescription, 'output/anomaly_report_des.html');
} else {
  console.warn("No anomalies detected for 'incident_description'.");
}
generateGhostPost(anomaliesDesc, metadataDescription, ghostConfig);

filterConditions = [
    { field: 'report_type_code', operator: '==', value: 'II' },
    { field: 'supervisor_district', operator: '==', value: '8' },
  ];

   // Perform anomaly detection
   const { results: d2anomalies, metadata: metadataD2 }  = anomalyDetection(data, filterConditions, 'incident_category');
   await generateAnomalyHtml(d2anomalies,metadataD2, 'output/d8anomaly_report.html');

  console.log('Anomaly detection report generated: anomaly_report.html');
})();
