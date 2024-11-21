// dataAggregator.js
export const aggregateData = (data, dimension, filterConditions = []) => {
    const aggregatedData = {};
  
    // Filter data based on `filterConditions`
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
            return true;
        }
      });
    });
  
    // Aggregate data by Year-Month and the chosen dimension
    filteredData.forEach((item) => {
      const yearMonth = `${item.year}-${String(item.month).padStart(2, '0')}`;
      const key = item[dimension] || 'Unknown';
  
      if (!aggregatedData[key]) {
        aggregatedData[key] = {};
      }
  
      if (!aggregatedData[key][yearMonth]) {
        aggregatedData[key][yearMonth] = 0;
      }
      aggregatedData[key][yearMonth] += parseInt(item.count);
    });
  
    // Fill in missing months with zero
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
  
  export const getAllYearMonthCombinations = (data) => {
    const uniqueYearMonths = new Set();
    data.forEach((item) => {
      const yearMonth = `${item.year}-${String(item.month).padStart(2, '0')}`;
      uniqueYearMonths.add(yearMonth);
    });
    return Array.from(uniqueYearMonths).sort((a, b) => new Date(a + '-01') - new Date(b + '-01'));
  };
  