import fetch from 'node-fetch'; // Ensure you have installed `node-fetch`
import { URL } from 'url'; // For handling URLs in Node.js

// Cleaning function for the query string
const cleanQueryString = (query) => {
    return query
        .replace(/\n/g, ' ')        // Replace line breaks with spaces
        .replace(/\s+/g, ' ')       // Replace multiple spaces with a single space
        .trim();                    // Trim leading and trailing whitespace
};

export const fetchDataFromAPI = async (queryObject) => {
    const baseUrl = "https://data.sfgov.org/resource/";
    let allData = [];
    let limit = 1000; // Set the limit for each request
    let offset = 0; // Initialize offset

    const { endpoint, query } = queryObject;
    if (!endpoint || !query) {
        console.error("Invalid query object:", queryObject);
        return null; // Handle invalid query object
    }

    // Clean the query string to remove unnecessary escape sequences
    const cleanedQuery = cleanQueryString(query);

    // Check if the cleaned query already includes a LIMIT clause
    const hasLimit = cleanedQuery.toLowerCase().includes("limit");
    let url;

    if (hasLimit) {
        // If the query already contains a LIMIT, construct the URL directly
        url = new URL(baseUrl + endpoint);
        url.searchParams.append("$query", cleanedQuery);
        console.log("URL being requested (with LIMIT):", url.href); // Debug output

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            allData = await response.json();
        } catch (error) {
            console.error("Failed to fetch data:", error);
            return null; // Return null or appropriate error handling
        }
    } else {
        // If no LIMIT is specified, handle pagination
        let hasMoreData = true;
        while (hasMoreData) {
            const paginatedQuery = `${cleanedQuery} LIMIT ${limit} OFFSET ${offset}`;
            url = new URL(baseUrl + endpoint);
            url.searchParams.append("$query", paginatedQuery);
            console.log("URL being requested (paginated):", url.href); // Debug output

            try {
                const response = await fetch(url);
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                const data = await response.json();
                allData = allData.concat(data);

                if (data.length < limit) {
                    hasMoreData = false; // Stop if the data received is less than the limit
                } else {
                    offset += limit; // Increase offset for the next batch
                }
            } catch (error) {
                console.error("Failed to fetch data:", error);
                return null; // Return null or appropriate error handling
            }
        }
    }

    return {
        data: allData,
        queryURL: url ? url.href : null, // Return the actual query URL used
    };
};
