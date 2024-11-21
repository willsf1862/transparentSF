import fs from 'fs';
import path from 'path';
import { addYears, format, startOfYear, endOfYear, addDays } from 'date-fns';
import { fetchDataFromAPI } from './api.js'; // Adjust the path based on your file structure
import { getVeryLongTermData } from './queries.mjs';

// Ensure the 'data' directory exists
const ensureDataDirectory = () => {
    console.log('Ensuring the data directory exists...');
    const dataDir = path.join(process.cwd(), 'data');
    if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir);
        console.log('Data directory created.');
    } else {
        console.log('Data directory already exists.');
    }
};

// Function to calculate date ranges for 1-year chunks
const calculate1YearChunks = () => {
    console.log('Calculating date ranges for 1-year chunks...');
    const startDate = startOfYear(new Date('2003-01-01'));
    const endDate = startOfYear(new Date('2018-12-31')); // Include 2017 fully
    
    const dateRanges = [];
    let currentStartDate = startDate;

    while (currentStartDate < endDate) { // Adjusted condition to ensure proper exit
        console.log(`Current start date: ${format(currentStartDate, 'yyyy-MM-dd')}`);
        let currentEndDate = addYears(currentStartDate, 1);
        if (currentEndDate > endDate) {
            currentEndDate = endDate;
        }
        console.log(`Current end date: ${format(currentEndDate, 'yyyy-MM-dd')}`);
        dateRanges.push({
            start: currentStartDate,
            end: currentEndDate,
        });
        currentStartDate = addDays(currentEndDate, 1); // Move start date to the next day after current end date
        console.log(`Updated start date for next iteration: ${format(currentStartDate, 'yyyy-MM-dd')}`);
    }

    console.log('Date ranges calculated:', dateRanges);
    return dateRanges;
};

// Function to fetch data for a given range with a timeout
const fetchAllDataForRange = async (queryObject) => {
    const timeout = (ms) => new Promise((_, reject) => setTimeout(() => reject(new Error('Request timed out')), ms));
    try {
        console.log(`Starting data fetch for queryObject:`, queryObject); // Log before fetch attempt
        const response = await Promise.race([fetchDataFromAPI(queryObject), timeout(10000)]); // 10 second timeout
        console.log(`Received response for queryObject`); // Log after fetch attempt
        if (response && response.data && Array.isArray(response.data) && response.data.length > 0) {
            console.log(`Fetched ${response.data.length} records for the range.`);
            return response.data;
        } else {
            console.warn(`No data available for the range.`);
            return [];
        }
    } catch (error) {
        console.error(`Error fetching data:`, error);
        return [];
    }
};

// Main function to fetch data in 1-year chunks
const main = async (district = null) => {
    try {
        console.log('Starting the script...');

        // Ensure the 'data' directory exists
        ensureDataDirectory();

        const dateRanges = calculate1YearChunks();

        for (const range of dateRanges) {
            const { start, end } = range;
            console.log(`Fetching data from ${format(start, 'yyyy-MM-dd')} to ${format(end, 'yyyy-MM-dd')}`);
            
            // Fetch data using the existing API function and query for each 1-year chunk
            const queryObject = getVeryLongTermData(start, end, district);
            console.log(`Query object prepared:`, queryObject); // Log the prepared queryObject before calling the API
            const sfData = await fetchAllDataForRange(queryObject);

            if (sfData && sfData.length > 0) {
                // Save data incrementally
                const filename = path.join('data', `data_${format(start, 'yyyy-MM-dd')}_to_${format(end, 'yyyy-MM-dd')}.json`);
                fs.writeFileSync(filename, JSON.stringify(sfData, null, 2));
                console.log(`Data successfully stored in ${filename}`);
            } else {
                console.warn(`No data returned for range ${format(start, 'yyyy-MM-dd')} to ${format(end, 'yyyy-MM-dd')}`);
            }
        }

        console.log('All data fetched and stored successfully.');

    } catch (error) {
        console.error('Error during data fetching and processing:', error);
    }
};

// Execute the main function
main();
