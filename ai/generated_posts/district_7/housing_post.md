### Trend 1: Decrease in Registered Business Locations in District 7

1. **Pattern:** Since 2014, District 7 has experienced a significant decline in registered business locations, with a 44% decrease observed from 1,027 in 2014 to 574 in 2024.

2. **Example:** In December 2024, the total business count decreased by 5% from the previous year, not reaching even two-thirds of the mid-2014 numbers.

3. **Chart:** 
   - [Registered Business Locations by Year](../static/chart_a9968a.png)

4. **Query URL:** 
   - [Query to Full Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

5. **Compelling Chart:** 
   - [Count by Year by Supervisor District](../static/chart_c6d24c.png) illustrating the decline by district.

### Trend 2: Rise in Notices of Violation

1. **Pattern:** Notices of Violation issued by the Department of Building Inspection have risen substantially by 89% from 2014 to 2024 in District 7.

2. **Example:** In 2024, the total Notices of Violation increased by 30% to 879 compared to 2023, with a notable 175% rise in violations linked to the building section.

3. **Chart:**
   - [Notices of Violation by Year](../static/chart_2cca6d.png)

4. **Query URL:**
   - [Query to Full Data](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)

5. **Compelling Chart:**
   - [Count by Year by Category](../static/chart_b6a65450.png) showing increase in building-related issues.

### Questions for Analyst:
- What are the primary factors contributing to the reduction in business registrations?
- What specific types of violations in the building section categories have shown the most significant increase?