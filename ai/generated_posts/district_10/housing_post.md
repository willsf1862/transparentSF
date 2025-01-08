Here are some factual observations and trends affecting District 10's housing and building developments, drawing from our search results:

1. **Notices of Violation Trends:**
   - Long-term Pattern: District 10 saw fluctuations in notices of violations related to housing and building inspections from 2014 to 2024.
   - Specific Example: In 2024, the total count of notices issued was 1,296, marking a 25% decrease from the 2023 total of 1,723.
   - Charts: [Count by Year](../static/chart_dd09a7.png)
   - Query: [Data Source](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)

2. **Registered Business Locations:**
   - Long-term Pattern: Business registrations have been on a consistent decline. As of 2024, the registration count was 854, down 15% from 2023.
   - Specific Example: In December 2024, business registrations in Bayview Hunters Point were 496, reflecting a 31% reduction below the 10-year average.
   - Charts: [Count by Year](../static/chart_1de4dc.png), [Count by Neighborhoods](../static/chart_6fb262.png)
   - Query: [Data Source](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

Each example supports a broader narrative about housing and building trends, particularly how regulatory attention and economic activity have shifted in District 10 over recent years. These points can serve as a foundation for a deeper dive into housing policy, business environment, and community impacts in upcoming reports.