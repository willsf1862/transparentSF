### Significant Long-term Trends in Housing for District 6 (2014-2024)

**1. Overarching Trend: Decline in Business Registrations.**
   - The total business locations in District 6 have continuously decreased from a high of 3,332 in 2019 to 1,489 in 2024. This represents a significant 44% drop below the district's 10-period average.
   - A clear demonstration of this trend is the change in business registrations for sectors like Food Services, which decreased by 18% in 2024 compared to 2023.

   **Concrete Examples:**
   - **South of Market Neighborhood**: Businesses here saw a stark decline, with a registration count of 499 in 2024, down from the 10-period average of 1,052. This marks a 52.6% decrease.

   **Supporting Chart Links:**
   - [Registered Business Locations - Count by Year](../static/chart_46c5d2.png)
   - [Registered Business Locations - South of Market Anomaly](../static/chart_5ade7dbe.png)

   **Query URL:**
   - [Full Query for Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

**2. Overarching Trend: Increased Notices of Violation in Uphill Trend**
   - Although the cumulative notices have seen variations, active violations surged by 242% in 2024 compared to 2023.

   **Concrete Examples:**
   - **Active Violations**: Moving from merely 109 counts in 2023 to 373 counts in 2024, defying the overall downward trend in total notices.

   **Supporting Chart Links:**
   - [Notices of Violation - Count by Year](../static/chart_279271.png)
   - [Active Violation Anomaly](../static/chart_7323731f.png)

   **Query URL:**
   - [Full Query for Notices of Violation](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)

**Questions for Analysts:**
1. What specific factors have contributed to the sharp decline in business registrations over the years, particularly in the South of Market neighborhood?
2. What key violations drove the surge in active notices despite an overall downward trend in filed notices?
3. Are there specific economic or policy influences tied to these trends in business registration and violation notices?

The examination of these data-backed trends unveils clear shifts in the business and regulatory landscape of District 6, providing a thorough lens into the region's evolving housing dynamics.