### Long-Term Housing Trends in District 5

**1. Decline in Registered Business Locations:**
   - **Pattern:** A significant decline in registered business locations within District 5 over the past decade suggests a diminishing commercial presence.
   - **Example:** The number of registered business locations decreased from 1,550 in 2014 to 791 in 2024, a 49% reduction.
   - **Data Query:** [View Raw Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   - **Chart:** 
     ![Registered Business Locations - San Francisco](../static/chart_8f9206.png)

**2. Notices of Violation:**
   - **Pattern:** A 10-year declining trend in the number of construction violations, reflecting improved compliance.
   - **Example:** Notices of violation dropped from 3,303 in 2014 to 1,732 in 2024, decreasing by 48%.
   - **Data Query:** [View Raw Data](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)
   - **Chart:** 
     ![Notices of Violation](../static/chart_9c2f73.png)

**3. Business Shifts Across Industries:**
   - **Pattern:** While general business locations are declining, some sectors such as food services saw an increase.
   - **Example:** Food services observed a 29% increase in 2024 compared to the previous year.
   - **Data Query:** [View Raw Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   - **Chart:** 
     ![Business Locations by NAICS Description](../static/chart_d0256b.png)

---

### Questions for Analysts:

1. **Detailed Housing Construction Trends:** How has the exact breakdown of residential vs. non-residential building permits evolved over the last decade?

2. **Impact on Community Sentiment:** Are resident satisfaction surveys available that reflect community sentiment related to these business and housing trends?

3. **Socio-economic Impact:** How are decreases in business locations affecting local employment and the economy in District 5?

The trends illustrate significant transformations in District 5 over the past decade, captured through business registrations and construction violations. These dataset insights are available through the city's open data portal for further exploration and validation.