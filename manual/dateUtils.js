// dateUtils.js
import { subMonths, startOfMonth, endOfMonth, addMonths, subWeeks, subDays } from 'date-fns';


export const calculateDates = () => {
    const today = new Date();
    // Determine the last complete month
    const lastCompleteMonth = subMonths(startOfMonth(today), 1);
    
    // Set time to noon to prevent time zone shifts
    lastCompleteMonth.setHours(12, 0, 0, 0);
    
    // Define the recent period as the full last complete month
    const calculatedStartDateRecent = startOfMonth(lastCompleteMonth);
    const calculatedEndDateRecent = endOfMonth(lastCompleteMonth);
    calculatedEndDateRecent.setHours(12, 0, 0, 0);
    
    // Define the comparison period as the 52 weeks before the recent period
    const calculatedEndDateComparison = subDays(calculatedStartDateRecent, 1);
    const calculatedStartDateComparison = subDays(calculatedEndDateComparison, 364);
    
    // Calculate 'updated_on' as today
    const updatedOn = today;
    
    // Calculate 'next_update' as the first day of the next month
    const nextUpdate = startOfMonth(addMonths(today, 1));
    
    return {
        calculatedStartDateRecent,
        calculatedEndDateRecent,
        calculatedStartDateComparison,
        calculatedEndDateComparison,
        updatedOn,
        nextUpdate,
        lastCompleteMonth
    };
};

