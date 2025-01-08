### Significant Housing Trends in District 11 (2014-2024)

1. **Long-term Decrease in Registered Business Locations**
   - **Pattern:** Over a decade, District 11 has experienced a significant decrease in registered business locations. From a peak of 777 in 2016 to a low of 468 in 2024.
   - **Example:** In 2024, the count is 23% below the 10-year average of 607.
   - **Relevant Chart:** 
     - [Registered Business Locations Count by Year](../static/chart_d358ab.png)
     - **Query URL:** [Business Locations Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   
2. **Diverse Impact on Different Neighborhoods**
   - **Pattern:** Varying levels of impact among neighborhoods. Excelsior, Outer Mission, Oceanview/Merced/Ingleside exhibit significant declines in business count.
   - **Example:** In Oceanview/Merced/Ingleside, the count is 32% below the decade's average.
   - **Relevant Chart:** 
     - [Business Locations by Neighborhoods](../static/chart_fe7957.png)

3. **Decrease in Business Activities in Specific Sectors**
   - **Pattern:** Notable decrease in certain business sectors including NA and Accommodations.
   - **Example:** Accommodations sector saw a 36% decrease compared to 2023.
   - **Relevant Chart:** 
     - [Business Locations by Sector](../static/chart_133cd8.png)

### Questions for Analysts
- What are the primary drivers behind the decline in registered business locations in District 11?
- Could the observed trends correlate with broader citywide economic shifts?

These objective patterns elucidate the broader impact of economic activities in District 11 and invite further exploration to understand underlying factors. The present analysis suggests a need for targeted policy interventions to stabilize and potentially uplift these key business sectors within the district.