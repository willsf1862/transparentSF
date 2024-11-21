// Import necessary dependencies using ES6 module syntax
import fs from 'fs';
import { subWeeks, subDays, startOfWeek, subMonths, startOfMonth, endOfMonth } from 'date-fns';
import { fetchDataFromAPI } from './api.js'; // Adjust the path based on your file structure
import { processData } from './dataProcessing.js';
import { getLongTermData } from './queries.mjs';

// Calculate date ranges for previous and comparison periods
const calculateDates = () => {
    const today = new Date();

    // Determine the last complete month
    const lastCompleteMonth = subMonths(startOfMonth(today), 1);
    const calculatedStartDateRecent = new Date('2018-01-01'); // Fixed start date
    const calculatedEndDateRecent = endOfMonth(lastCompleteMonth); // End date is the last day of the last complete month

    return {
        calculatedStartDateRecent,
        calculatedEndDateRecent,
    };
};

const main = async (district = null) => {
    try {
        console.log('Starting the script...');
        
        // Calculate date ranges
        const {
            calculatedStartDateRecent,
            calculatedEndDateRecent,
        } = calculateDates();

        console.log('Calculated Dates:');
        console.log(`Start Date Recent: ${calculatedStartDateRecent}`);
        console.log(`End Date Recent: ${calculatedEndDateRecent}`);

        // Fetch data using the existing API function and query
        console.log('Fetching data...');
        const queryObject = getLongTermData(
            calculatedStartDateRecent,
            calculatedEndDateRecent,
            district
        );

        console.log('Query Object:', queryObject);

        const sfData = await fetchDataFromAPI(queryObject);
        console.log('Data fetched:', sfData ? `Fetched ${sfData.length} records.` : 'No data returned.');
        
        const filename = `data_${calculatedStartDateRecent.toISOString().split('T')[0]}_to_${calculatedEndDateRecent.toISOString().split('T')[0]}.json`;
        fs.writeFileSync(filename, JSON.stringify(sfData, null, 2));
        console.log(`Data successfully stored in ${filename}`);

        // Optional: Process data
        // if (sfData && sfData.length > 0) {
        //     console.log('Processing data...');
        //     const { groupedData, metadata } = processData(
        //         sfData,
        //         calculatedStartDateRecent,
        //         calculatedEndDateRecent
        //     );
        //     console.log('Data processed successfully.');
        // } else {
        //     console.error('No data fetched to process.');
        // }
    } catch (error) {
        console.error('Error during data fetching and processing:', error);
    }
};

// Execute the main function
main();
