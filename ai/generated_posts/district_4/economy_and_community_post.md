### Economic Trends in District 4: Business Registrations

1. **Overarching Pattern**: Business registrations in District 4 have consistently decreased over the past decade. As of December 2024, there was a notable decline of 33% from the 10-year average. The count stood at 463, an 18% decrease from the previous year's total of 568.
   
2. **Specific Example**: The Sunset/Parkside neighborhood, a key area in District 4, saw its business registrations drop to 457, which is 18% below the 2023 total and 32.5% below the 10-year average.

3. **Charts**:
   - [Count by Year Chart](../static/chart_7c13e9.png)
   - [Count by Year by Supervisor District Chart](../static/chart_cc5b373f.png)
   - [Count by Year by Neighborhoods Analysis Chart](../static/chart_8e2dcaea.png)

4. **Query URL**:
   - [Data Query for Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

5. **Compelling Chart**: 
   - ![Chart of Count by Year](../static/chart_7c13e9.png) - This chart illustrates the dramatic decline in business registrations over the years, making the data visual and compelling.

### Questions for Analysts:
- What factors contributed to the decline in business registrations in District 4, especially in key neighborhoods like Sunset/Parkside?
- Are there specific industries within District 4 that have been more resilient or more affected by this trend?
- How does District 4 compare to other districts in terms of economic growth trends?