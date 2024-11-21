// Import required modules
import fs from 'fs/promises';
import { createReadStream } from 'fs';  // Import createReadStream for streaming files
import fsSync from 'fs';
import path from 'path';
import { Readable } from 'stream';
import puppeteer from 'puppeteer';
import GhostAdminAPI from '@tryghost/admin-api';
import tmp from 'tmp';
import FormData from 'form-data';
import { generateChartHtml } from './chartGenerator.js';  // Import the shared function

/**
 * Helper function to check if a period covers the entire month
 * 
 * @param {Object} period - An object with `start` and `end` properties in the format 'YYYY-MM-DD'.
 * @returns {boolean} - True if the period covers the entire month, false otherwise.
 */
const isEntireMonth = (period) => {
    const [startYear, startMonth, startDay] = period.start.split('-').map(Number);
    const [endYear, endMonth, endDay] = period.end.split('-').map(Number);

    // Check if start is first day of the month
    const isStartFirstDay = startDay === 1;

    // Check if end is the last day of the month
    const lastDay = new Date(startYear, startMonth, 0).getDate(); // Get last day of the month (1-based)
    const isEndLastDay = endDay === lastDay;

    // Also check if start and end are in the same month and year
    const isSameMonthYear = (startMonth === endMonth) && (startYear === endYear);

    return isStartFirstDay && isEndLastDay && isSameMonthYear;
};

// Helper function to clean up all images in the output directory
const cleanUpOutputFolder = async () => {
    try {
        const files = await fs.readdir('output');
        for (const file of files) {
            await fs.unlink(path.join('output', file));
        }
        console.log('All images in output folder have been deleted.');
    } catch (error) {
        console.error('Error cleaning up output folder:', error);
    }
};

/**import fs from 'fs/promises';
import path from 'path';
import tmp from 'tmp';
import puppeteer from 'puppeteer';
import GhostAdminAPI from '@tryghost/admin-api';
import { createCanvas } from 'canvas';
import fetch from 'node-fetch';

/**
 * Uploads an image to Ghost and returns the image URL.
 *
 * @param {Buffer} imageBuffer - The image data as a buffer.
 * @param {string} imageName - The name to assign to the uploaded image.
 * @param {GhostAdminAPI} api - The initialized GhostAdminAPI instance.
 * @returns {string} - The URL of the uploaded image.
 */
const uploadImageToGhost = async (imageBuffer, imageName, api) => {
    const tempFile = tmp.fileSync({ postfix: path.extname(imageName) });
    const tempPath = tempFile.name;

    try {
        // Write the image buffer to the temporary file
        await fs.writeFile(tempPath, imageBuffer);
        console.log(`Image written to temporary path: ${tempPath}`);

        // Upload the image using the file path
        const response = await api.images.upload({
            file: tempPath,
            name: imageName
        });

        console.log(`Image successfully uploaded to Ghost: ${response.url}`);
        return response.url;
    } catch (error) {
        console.error('Error uploading image to Ghost:', error);
        throw error;
    } finally {
        tempFile.removeCallback();
        console.log(`Temporary image file deleted: ${tempPath}`);
    }
};

/**
 * Generates a chart image using Puppeteer and Plotly.
 *
 * @param {Object} item - The anomaly object.
 * @param {string} chartTitle - The title for the chart.
 * @param {Object} metadata - Metadata object containing periods and filter conditions.
 * @param {Object} browser - The Puppeteer browser instance.
 * @returns {Buffer} - The image buffer of the chart.
 */
/**
 * Generates a chart image using Puppeteer and Plotly, and extracts the caption.
 *
 * @param {Object} item - The anomaly object.
 * @param {string} chartTitle - The title for the chart.
 * @param {Object} metadata - Metadata object containing periods and filter conditions.
 * @param {Object} browser - The Puppeteer browser instance.
 * @returns {Object} - An object containing the image buffer of the chart and the extracted caption.
 */
const generateChartImageBuffer = async (item, chartTitle, metadata, browser) => {
    try {
        // Generate the chart HTML, now including metadata
        const chartHtml = generateChartHtml(item, chartTitle, 'chart', metadata);
        console.log("Generated Chart HTML:", chartHtml); // Debugging statement

        // Extract caption using improved regex
        const captionMatch = chartHtml.match(/<div[^>]*id=["']chart-caption["'][^>]*>([\s\S]*?)<\/div>/);
        const caption = captionMatch ? captionMatch[1].trim() : 'No caption available';
        //console.log("Extracted Caption:", caption); // Debugging statement

        const htmlContent = `
            <html>
            <head>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            </head>
            <body>
                ${chartHtml}
            </body>
            </html>
        `;

        const page = await browser.newPage();
        await page.setContent(htmlContent, { waitUntil: 'networkidle0' });

        await page.waitForSelector('#chart', { timeout: 10000 });
        const chartElement = await page.$('#chart');

        if (!chartElement) {
            throw new Error(`Chart element not found for ${chartTitle}`);
        }

        const buffer = await chartElement.screenshot({ omitBackground: true });
        await page.close();

        return { buffer, caption };
    } catch (error) {
        console.error(`Error in generateChartImageBuffer for ${chartTitle}:`, error);
        throw error;
    }
};


/**
 * Generates Ghost posts with dynamic titles and embedded charts for each anomaly category.
 *
 * @param {Array} anomalies - Array of anomaly objects.
 * @param {Object} metadata - Metadata object containing periods and filter conditions.
 * @param {Object} ghostConfig - Configuration object for Ghost API access.
 */
export const generateGhostPost = async (anomalies, metadata, ghostConfig) => {
    if (!anomalies || anomalies.length === 0) {
        console.error('No anomalies to report.');
        return;
    }

    //console.log("metadata:", metadata);

    const recentPeriodEntireMonth = isEntireMonth(metadata.recentPeriod);
    const formattedMetadata = {
        recentPeriod: {
            start: metadata.recentPeriod.start,
            end: metadata.recentPeriod.end,
            entireMonth: recentPeriodEntireMonth,
            monthYear: recentPeriodEntireMonth ? new Date(metadata.recentPeriod.end).toLocaleString('default', { month: 'long', year: 'numeric' }) : null
        },
        comparisonPeriod: {
            start: metadata.comparisonPeriod.start,
            end: metadata.comparisonPeriod.end,
        },
        filterConditions: metadata.filterConditions
    };

    const groupField = Object.keys(anomalies[0]).find(key => !['comparison_mean', 'comparison_stdDev', 'recent_mean', 'difference', 'out_of_bounds', 'dates', 'counts', 'comparisonPeriod', 'recentPeriod'].includes(key));
    if (!groupField) {
        console.error('No valid group field found in anomalies data.');
        return;
    }

    const api = new GhostAdminAPI({
        url: ghostConfig.url,
        key: ghostConfig.adminApiKey,
        version: 'v5'
    });

    const browser = await puppeteer.launch();

    try {
        for (let i = 0; i < anomalies.length; i++) {
            const item = anomalies[i];
            if (item.out_of_bounds) {
                try {
                    // Create a descriptive title for the post and chart, now including 'San Francisco'
                    const percentDifference = Math.floor((item.difference / item.comparison_mean) * 100); // Limiting percentage to integer portion
                    const action = percentDifference > 0 ? 'Increase' : 'Decrease';
                    const chartTitle = `San Francisco: ${Math.abs(percentDifference)}% ${action} in Police Incidents of ${item[groupField]}`;

                    // Generate the chart image buffer using Puppeteer and extract the caption
                    const { buffer: imageBuffer, caption } = await generateChartImageBuffer(item, chartTitle, formattedMetadata, browser);

                    // Upload the image to Ghost and get the URL
                    const imageUrl = await uploadImageToGhost(imageBuffer, `chart-${item[groupField]}.png`, api);
                    console.log(`Uploaded image for ${item[groupField]}: ${imageUrl}`);

                    // Prepare the footnote with the dataset link and filter conditions
                    const footnote = `*Data source: [Police Incident Report Dataset](LINK_TO_DATASET). Filter conditions applied: ${formattedMetadata.filterConditions || 'None'}*`;

                    // Prepare the Mobiledoc for the Ghost post
                    const mobiledoc = {
                        version: "0.3.1",
                        atoms: [],
                        cards: [
                            ["image", {
                                "src": imageUrl,
                                "caption": chartTitle
                            }]
                        ],
                        markups: [],
                        sections: [
                            [1, "p", [
                                [0, [], 0, caption]  // Using the extracted caption from the HTML
                            ]],
                            [10, 0],  // This references the image card by index in cards array
                            [1, "p", [
                                [0, [], 0, footnote]  // Adding the footnote under the chart
                            ]]
                        ]
                    };

                    // Create a new post with the generated content
                    const newPost = await api.posts.add({
                        title: chartTitle,
                        mobiledoc: JSON.stringify(mobiledoc),
                        status: 'draft'
                    });

                    console.log(`Ghost post created: ${newPost.url}`);
                } catch (error) {
                    console.error(`Failed to generate/upload post for ${item[groupField]}:`, error);
                }
            }
        }
    } finally {
        await browser.close();
        console.log('All Ghost posts have been processed and output folder cleaned up.');
    }
};
