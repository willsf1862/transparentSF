// anomalyDetection.js
export const getDateRanges = () => {
    const today = new Date();
  
    // Calculate recent period: last full month (previous month)
    const recentEnd = new Date(today.getFullYear(), today.getMonth(), 0); // Last day of previous month
    const recentStart = new Date(recentEnd.getFullYear(), recentEnd.getMonth(), 1); // First day of previous month
  
    // Calculate comparison period: 12 months before recentStart
    const comparisonEnd = new Date(recentStart.getTime() - 1); // Day before recentStart
    const comparisonStart = new Date(recentStart.getFullYear() - 1, recentStart.getMonth(), 1); // Same month and day, previous year
  
    // Log the date ranges
    console.log('Recent Period Start:', recentStart.toISOString().split('T')[0]);
    console.log('Recent Period End:', recentEnd.toISOString().split('T')[0]);
    console.log('Comparison Period Start:', comparisonStart.toISOString().split('T')[0]);
    console.log('Comparison Period End:', comparisonEnd.toISOString().split('T')[0]);
  
    return {
      recentPeriod: {
        start: recentStart,
        end: recentEnd,
      },
      comparisonPeriod: {
        start: comparisonStart,
        end: comparisonEnd,
      },
    };
  };
  
  export const groupDataByFieldAndDate = (dataArray, groupField) => {
    const grouped = {};
    dataArray.forEach((item) => {
      const key = item[groupField] || 'Unknown';
      const dateKey = `${item.year}-${item.month.padStart(2, '0')}`;
      if (!grouped[key]) {
        grouped[key] = {};
      }
      if (!grouped[key][dateKey]) {
        grouped[key][dateKey] = 0;
      }
      grouped[key][dateKey] += parseInt(item.count);
    });
    return grouped;
  };
  
  export const filterDataByDateAndConditions = (data, startDate, endDate, filterConditions) => {
    let dataArray;
  
    // Check if data is an object, then convert it to an array of its values
    if (Array.isArray(data)) {
      dataArray = data;
    } else if (typeof data === 'object') {
      dataArray = Object.values(data);
    } else {
      throw new Error("Expected 'data' to be an array or an object.");
    }
  
    // Proceed to filter the data array
    return dataArray.filter((item) => {
      // Construct a date from the 'year' and 'month' fields
      const itemDate = new Date(parseInt(item.year), parseInt(item.month) - 1, 1); // Months are zero-based
  
      return (
        itemDate >= startDate &&
        itemDate <= endDate &&
        filterConditions.every(({ field, operator, value }) => {
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
              return true;
          }
        })
      );
    });
  };
  
  
  export const calculateStats = (values) => {
    //console.log("Values:", values);
    const n = values.length;
    const mean = values.reduce((a, b) => a + b, 0) / n;
    const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / n;
    const stdDev = Math.sqrt(variance);
    return { mean, stdDev };
  };
  
  export const anomalyDetection = (data, filterConditions = [], groupField = 'incident_description') => {
    const { recentPeriod, comparisonPeriod } = getDateRanges();
    console.log('Filter Conditions:', filterConditions);
    console.log('Group Field:', groupField);
    console.log('Data Length:', data.length);
    console.log('Data Type', typeof data);
    // Filter data for recent and comparison periods
    const recentData = filterDataByDateAndConditions(
      data,
      comparisonPeriod.start, // We need data from comparison start to recent end for time series
      recentPeriod.end,
      filterConditions
    );
  
    const allGroupedData = groupDataByFieldAndDate(recentData, groupField);
  
    // Prepare results with time series data
    const results = [];
  
    Object.keys(allGroupedData).forEach((groupValue) => {
      const dataPoints = allGroupedData[groupValue];
  
      // Extract dates and counts
      const dates = Object.keys(dataPoints).sort();
      const counts = dates.map((date) => dataPoints[date]);
  
      // Separate data into comparison and recent periods
      const comparisonDates = dates.filter((date) => {
        const [year, month] = date.split('-').map(Number);
        const itemDate = new Date(year, month - 1, 1);
        return itemDate >= comparisonPeriod.start && itemDate <= comparisonPeriod.end;
      });
  
      const recentDates = dates.filter((date) => {
        const [year, month] = date.split('-').map(Number);
        const itemDate = new Date(year, month - 1, 1);
        return itemDate >= recentPeriod.start && itemDate <= recentPeriod.end;
      });
  
      const comparisonCounts = comparisonDates.map((date) => dataPoints[date]);
      const recentCounts = recentDates.map((date) => dataPoints[date]);
  
      // Only proceed if both comparisonCounts and recentCounts are not empty
      if (comparisonCounts.length > 0 && recentCounts.length > 0) {
        // Calculate stats
        const comparisonStats = calculateStats(comparisonCounts);
        const recentStats = calculateStats(recentCounts);
  
        // Ensure means are valid numbers
        if (!isNaN(comparisonStats.mean) && !isNaN(recentStats.mean)) {
          const difference = recentStats.mean - comparisonStats.mean;
          const outOfBounds =
            Math.abs(difference) > comparisonStats.stdDev * 3 && comparisonStats.mean > 2 && recentStats.mean > 2;
  
          results.push({
            [groupField]: groupValue,
            comparison_mean: comparisonStats.mean,
            comparison_stdDev: comparisonStats.stdDev,
            recent_mean: recentStats.mean,
            difference: difference,
            out_of_bounds: outOfBounds,
            dates,
            counts: dates.map((date) => dataPoints[date]),
            comparisonPeriod,
            recentPeriod,
          });
        }
      }
    });
  
    // Sort results by the magnitude of the difference
    results.sort((a, b) => {
      const diffA = Math.abs(a.difference);
      const diffB = Math.abs(b.difference);
  
      if (isNaN(diffA) && isNaN(diffB)) return 0;
      if (isNaN(diffA)) return 1;
      if (isNaN(diffB)) return -1;
  
      return diffB - diffA;
    });
     // Compile metadata
     const metadata = {
        recentPeriod: {
          start: recentPeriod.start.toISOString().split('T')[0],
          end: recentPeriod.end.toISOString().split('T')[0],
        },
        comparisonPeriod: {
          start: comparisonPeriod.start.toISOString().split('T')[0],
          end: comparisonPeriod.end.toISOString().split('T')[0],
        },
        filterConditions: filterConditions,
        titleField: groupField,
      };
    
      return {
        results,
        metadata,
      };
    };
    