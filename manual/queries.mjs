// queries.mjs
// queries.mjs

const getIncidentComparisonWithFiltersQuery = (
    startDateRecent,        // Date object
    filterConditions = [],   // Array of filter condition objects
    district,
) => {
    // Helper function to extract year and month from a Date object
    const getYearMonth = (date) => {
        if (!(date instanceof Date) || isNaN(date)) {
            throw new TypeError(`Invalid Date object: ${date}`);
        }
        const year = date.getFullYear();
        const month = date.getMonth() + 1; // getMonth() returns 0-11, so we add 1
        return { year, month };
    };

    // Extract month and year for recent period
    const { year: recentYear, month: recentMonth } = getYearMonth(startDateRecent);

    // Calculate the comparison year (previous year)
    const comparisonYear = recentYear - 1;

    // Operator mapping to handle different SQL operators
    const operatorMap = {
        '==': '=',
        '!=': '!=',
        '>': '>',
        '<': '<',
        '>=': '>=',
        '<=': '<=',
        'in': 'in',
    };

    // Helper function to build additional filter clauses
    const buildFilterClause = (conditions) => {
        console.log("conditions:", conditions);
        return conditions
            .map(({ field, operator, value }) => {
                const sqlOperator = operatorMap[operator] || '=';
                if (operator === 'in') {
                    if (!Array.isArray(value)) {
                        throw new TypeError(`Filter value for field '${field}' must be an array when using 'in' operator.`);
                    }
                    const sanitizedValues = value.map(v => `'${v.replace(/'/g, "''")}'`).join(', ');
                    return `${field} IN (${sanitizedValues})`;
                } else {
                    if (typeof value === 'object') {
                        throw new TypeError(`Filter value for field '${field}' is an object. Expected a primitive.`);
                    }
                    // Sanitize value to prevent injection
                    const sanitizedValue = value.replace(/'/g, "''");
                    return `${field} ${sqlOperator} '${sanitizedValue}'`;
                }
            })
            .join(' AND ');
    };

    // Base WHERE clauses for recent and comparison periods
    let whereClauseRecent = `date_extract_y(report_datetime) = ${recentYear} AND date_extract_m(report_datetime) = ${recentMonth}`;

    let whereClauseComparison = `date_extract_y(report_datetime) = ${comparisonYear} AND date_extract_m(report_datetime) = ${recentMonth}`;

    // Add additional filter conditions if any
    if (filterConditions.length > 0) {
        const additionalFilterRecent = buildFilterClause(filterConditions);
        const additionalFilterComparison = buildFilterClause(filterConditions);
        whereClauseRecent += ` AND ${additionalFilterRecent}`;
        whereClauseComparison += ` AND ${additionalFilterComparison}`;
    }
    
    if (district) {
        whereClauseRecent += ` AND supervisor_district = '${district}'`;
        whereClauseComparison += ` AND supervisor_district = '${district}'`;
    }
    // Construct the SoQL query
    const query = `
    SELECT 
        supervisor_district, 
        date_extract_y(report_datetime) AS year,
        date_extract_m(report_datetime) AS month, 
        COUNT(*) AS count
    WHERE 
        (${whereClauseRecent}) 
        OR (${whereClauseComparison})
    GROUP BY 
        supervisor_district, 
        year, 
        month
    ORDER BY 
        supervisor_district, 
        year, 
        month
    `;

    return {
        endpoint: "wg3w-h783.json",
        query: query
    };
};



const getCategoryComparisonQuery = (startDateRecent, endDateRecent, startDateComparison, endDateComparison) => {
    const getDaysBetween = (start, end) => {
        const startDate = new Date(start);
        const endDate = new Date(end);
        const differenceInTime = endDate.getTime() - startDate.getTime();
        return differenceInTime / (1000 * 3600 * 24) + 1; // Adding 1 to include both start and end date
    };

    const daysRecent = getDaysBetween(startDateRecent, endDateRecent);
    const daysComparison = getDaysBetween(startDateComparison, endDateComparison);

    let whereClauseRecent = `report_datetime >= '${startDateRecent.toISOString().replace('Z', '')}' AND report_datetime <= '${endDateRecent.toISOString().replace('Z', '')}'`;
    let whereClauseComparison = `report_datetime >= '${startDateComparison.toISOString().replace('Z', '')}' AND report_datetime <= '${endDateComparison.toISOString().replace('Z', '')}'`;

   
    return {
        endpoint: "wg3w-h783.json",
        query: `
            SELECT 
                supervisor_district, incident_category,
                CASE 
                    WHEN incident_category IN ('Assault', 'Homicide', 'Rape','Robbery') THEN 'Violent Crime'
                    WHEN incident_category IN ('Burglary', 'Malicious Mischief', 'Embezzlement', 'Larceny Theft', 'Stolen Property', 'Vandalism', 'Motor Vehicle Theft', 'Arson') THEN 'Property Crime'
                    ELSE 'Other Crime'
                END AS category_group,
                SUM(CASE 
                    WHEN ${whereClauseRecent} THEN 1 
                    ELSE 0 
                END) AS total_count_recent,
                SUM(CASE 
                    WHEN ${whereClauseComparison} THEN 1 
                    ELSE 0 
                END) AS total_count_comparison,
                SUM(CASE 
                    WHEN ${whereClauseRecent} THEN 1 
                    ELSE 0 
                END) / ${daysRecent} AS daily_average_recent,
                SUM(CASE 
                    WHEN ${whereClauseComparison} THEN 1 
                    ELSE 0 
                END) / ${daysComparison} AS daily_average_comparison,
                CASE
                    WHEN ${whereClauseRecent} THEN 'Recent Period'
                    WHEN ${whereClauseComparison} THEN 'Comparison Period'
                END AS period
            WHERE ((${whereClauseRecent}) OR (${whereClauseComparison})) AND report_type_code = 'II'
            GROUP BY incident_category, supervisor_district, category_group, period
            ORDER BY supervisor_district, category_group, incident_category, period`
    };
};

const getSupervisorQuery = () => ({
    endpoint: "cqbw-m5m3.json",
    query: `SELECT sup_dist_num, sup_name, multipolygon WHERE sup_dist_num IS NOT NULL order by sup_dist_num`
});

const getIncidentQuery = (startDateRecent, endDateRecent, startDateComparison, endDateComparison, district) => {
    let whereClauseRecent = `report_datetime >= '${startDateRecent.toISOString().replace('Z', '')}' AND report_datetime <= '${endDateRecent.toISOString().replace('Z', '')}'`;
    let whereClauseComparison = `report_datetime >= '${startDateComparison.toISOString().replace('Z', '')}' AND report_datetime <= '${endDateComparison.toISOString().replace('Z', '')}'`;

    if (district) {
        whereClauseRecent += ` AND supervisor_district = '${district}'`;
        whereClauseComparison += ` AND supervisor_district = '${district}'`;
    }

    return {
        endpoint: "wg3w-h783.json",
        query: `
            SELECT incident_category, supervisor_district, date_trunc_ymd(report_datetime) AS date, COUNT(*) AS count 
            WHERE ((${whereClauseRecent}) OR (${whereClauseComparison})) AND report_type_code = 'II'
            GROUP BY incident_category, supervisor_district, date 
            ORDER BY date`
    };
};

const getAnomalyQuery = (startDateRecent, endDateRecent, startDateComparison, endDateComparison, district) => {
    const startDateRecentStr = startDateRecent.toISOString().replace('Z', '');
    const endDateRecentStr = endDateRecent.toISOString().replace('Z', '');
    const startDateComparisonStr = startDateComparison.toISOString().replace('Z', '');
    const endDateComparisonStr = endDateComparison.toISOString().replace('Z', '');

    let whereClauseRecent = `report_datetime >= '${startDateRecentStr}' AND report_datetime <= '${endDateRecentStr}'`;
    let whereClauseComparison = `report_datetime >= '${startDateComparisonStr}' AND report_datetime <= '${endDateComparisonStr}'`;

    if (district) {
        whereClauseRecent += ` AND supervisor_district = '${district}'`;
        whereClauseComparison += ` AND supervisor_district = '${district}'`;
    }

    return {
        endpoint: 'wg3w-h783.json',
        query: `
            SELECT 
                incident_description,
                date_trunc_ymd(report_datetime) AS date,
                COUNT(distinct incident_number) AS count,
                CASE 
                    WHEN report_datetime >= '${startDateRecentStr}' AND report_datetime <= '${endDateRecentStr}' THEN 'recent'
                    ELSE 'comparison'
                END AS period
            WHERE (${whereClauseRecent}) OR (${whereClauseComparison}) and report_type_code = 'II'
            GROUP BY incident_description, date, period
            ORDER BY incident_description, date
        `,
    };
};

const getHomicideQuery = (fromDate, toDate, category) => {
    const whereClause = `report_datetime >= '${fromDate}' AND report_datetime <= '${toDate}' AND incident_category = 'Homicide'`;

    return {
        endpoint: "wg3w-h783.json",
        query: `
            SELECT distinct Incident_id as count
            WHERE ${whereClause}
            ORDER BY count DESC`
    };
};

const getEmployeeSalaryQuery = (employeeName) => ({
    endpoint: "88g8-5mnd.json", // Adjust the endpoint if necessary
    query: `
        SELECT 
            employee_identifier,
            job,
            year,
            SUM(total_salary) AS total_salary,
            SUM(total_benefits) AS total_benefits,
            SUM(total_compensation) AS total_compensation
        WHERE 
            employee_identifier = '${employeeName}' AND 
            year_type = 'Calendar'
        GROUP BY year, employee_identifier, job
        ORDER BY year ASC
    `
});

const getLongTermData = (startDateRecent, endDateRecent, district) => {
    let whereClauseRecent = `report_datetime >= '${startDateRecent.toISOString().replace('Z', '')}' AND report_datetime <= '${endDateRecent.toISOString().replace('Z', '')}'`;

    if (district) {
        whereClauseRecent += ` AND supervisor_district = '${district}'`;
    }

    return {
        endpoint: "wg3w-h783.json",
        query: `
            SELECT 
                supervisor_district, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                date_extract_y(report_datetime) AS year,
                date_extract_m(report_datetime) AS month, 
                COUNT(*) AS count, 
                CASE 
                    WHEN incident_category IN ('Assault', 'Homicide', 'Rape', 'Robbery') THEN 'Violent Crime'
                    WHEN incident_category IN ('Burglary', 'Malicious Mischief', 'Embezzlement', 'Larceny Theft', 'Stolen Property', 'Vandalism', 'Motor Vehicle Theft', 'Arson') THEN 'Property Crime'
                    ELSE 'Other Crime'
                END AS category_group
            WHERE ${whereClauseRecent}
            GROUP BY 
                category_group, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                supervisor_district, 
                year, 
                month
            ORDER BY 
                category_group, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                supervisor_district, 
                year, 
                month
        `
    };
};

const getVeryLongTermData = (startDateRecent, endDateRecent, district) => {
    let whereClauseRecent = `date >= '${startDateRecent.toISOString().replace('Z', '')}' AND date <= '${endDateRecent.toISOString().replace('Z', '')}'`;

    if (district) {
        whereClauseRecent += ` AND supervisor_district = '${district}'`;
    }

    return {
        endpoint: "tmnf-yvry.json",
        query: `
            SELECT 
                Category, 
                Descript, 
                incident_code, 
                date_extract_y(date) AS year,
                date_extract_m(date) AS month, 
                COUNT(*) AS count, 
                CASE 
                    WHEN category IN ('Assault', 'Homicide', 'Rape', 'Robbery') THEN 'Violent Crime'
                    WHEN category IN ('Burglary', 'Malicious Mischief', 'Embezzlement', 'Larceny Theft', 'Stolen Property', 'Vandalism', 'Motor Vehicle Theft', 'Arson') THEN 'Property Crime'
                    ELSE 'Other Crime'
                END AS category_group
            WHERE ${whereClauseRecent}
            GROUP BY 
                category_group, 
                Category, 
                descript,
                incident_code,
                year, 
                month
            ORDER BY 
                category_group, 
                Category, 
                Descript,
                incident_code,
                year, 
                month
        `
    };
};

const getCurrentMayorQuery = () => {
    return {
        endpoint: "88g8-5mnd.json",
        query: `
            SELECT
                organization_group_code,
                job_family_code,
                job_code,
                year_type,
                year,
                organization_group,
                department_code,
                department,
                union_code,
                \`union\`,
                job_family,
                job,
                employee_identifier,
                salaries,
                overtime,
                other_salaries,
                total_salary,
                retirement,
                health_and_dental,
                other_benefits,
                total_benefits,
                total_compensation,
                hours,
                employment_type,
                data_as_of,
                data_loaded_at
            WHERE
                job = 'Mayor'
                AND year_type = 'Calendar'
            ORDER BY year DESC
            LIMIT 1
        `,
    };
};



// Exporting all functions using ES module syntax
export {
    getIncidentComparisonWithFiltersQuery,
    getCategoryComparisonQuery,
    getSupervisorQuery,
    getIncidentQuery,
    getAnomalyQuery,
    getHomicideQuery,
    getEmployeeSalaryQuery,
    getLongTermData,
    getVeryLongTermData,
    getCurrentMayorQuery // Export the new function
};