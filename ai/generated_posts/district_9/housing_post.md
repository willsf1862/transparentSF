### Significant Long-term Trends in Housing in District 9

#### 1. Notices of Violation in Housing

**Long-term Pattern:** There has been a significant decrease in the Notices of Violation issued by the Department of Building Inspection in District 9 for housing from 2014 to 2024.

**2024 Anomaly:** The total count for 2024 sits at 1,456, marking a 41% decrease compared to the 2023 total of 2,449.

**Specific Example:** For the "Building Section" category, the count in 2024 was 486, down 52% from 1,010 in 2023.

**Supporting Charts and Data:**

- **Query URL**: [Notices of Violation issued by the Department of Building Inspection](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)
- **Chart**: ![Notices of Violation by Year](../static/chart_925fb1.png)

**Questions for Analysts:**
- What underlying factors contributed to the decrease in violation notices?
- Are there specific policies or initiatives that have led to compliance improvements?


#### 2. Business Locations and Housing Availability

**Long-term Pattern:** Registration of new business locations in District 9 has been declining, impacting the utilization of available spaces for commercial purposes, which might increase the potential for housing conversion.

**2024 Anomaly:** In 2024, the total registered business count was 1,024, which is 10% below the 2023 total of 1,135.

**Specific Example:** The number of registered business locations in the Mission neighborhood was 726 in 2024, which is 8% below the 2023 total of 785.

**Supporting Charts and Data:**

- **Query URL**: [Registered Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
- **Chart**: ![Registered Business Locations by Year](../static/chart_6f6fe5.png)

**Questions for Analysts:**
- What are the main factors leading to the decline in business registrations?
- How is the availability of business spaces affecting housing supply and demand dynamics in District 9?

These insights reveal important shifts in housing and commercial activities in District 9, with potential implications for community development and urban planning.