### Long-term Trends in Health and Social Services in District 6

1. **Trend: Decrease in Registered Business Locations**
   - **Pattern:** There has been a steady decline in registered business locations within District 6 over the last decade.
   - **Examples:** 
      - By December 2024, the total count of registered business locations was 1,489, 14% below the 2023 total of 1,738 and 44% below the 10-year average of 2,654.
      - The South of Market neighborhood saw a sharp decrease to 499, which is 53% below the 10-year average of 1,052.
   - **Query URL:** [Registered Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   - **Chart:** ![Chart](../static/chart_f7300f.png)
   - **Questions for Analysts:** 
     - What factors led to the significant decline in business registrations in District 6?
     - What industries were most affected by these changes?

2. **Trend: Shift in Law Enforcement Dispatched Calls**
   - **Pattern:** An increase in dispatched calls for service in District 6 over the long term, though with fluctuations in call types.
   - **Examples:** 
      - As of December 2024, total call count was 93,843, a 13% increase compared to the YTD 2023 total.
      - Traffic Violation Cite saw an increase of 113% above the 10-period average.
   - **Query URL:** [Law Enforcement Dispatched Calls](https://data.sfgov.org/resource/2zdj-bwza.json?%24query=SELECT+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+date_trunc_y%28received_datetime%29+AS+year+ORDER+BY+year)
   - **Chart:** ![Chart](../static/chart_e65338.png)
   - **Questions for Analysts:** 
     - What specific factors have influenced the rise in call counts?
     - Which areas within District 6 are experiencing the most significant increase in specific call types?

3. **Trend: Reduction in Incident Counts**
   - **Pattern:** A decrease in the overall police incident reports over the years, particularly in Property Crime.
   - **Examples:** 
      - In 2024, the total incident count was 17,472, which is 14% below the 2023 total.
      - Property Crime incidents were 36% below the 6-period average.
   - **Query URL:** [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year)
   - **Chart:** ![Chart](../static/chart_fc8ebaf6.png)
   - **Questions for Analysts:**
     - What categories within property crime have seen the largest reduction?
     - What measures, if any, have contributed to this decline in incident reports?

These findings draw attention to both social trends impacting business environments and various facets of community interaction with social services, led mainly by law enforcement attributes.