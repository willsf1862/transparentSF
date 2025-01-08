### Long-Term Trends in Economy and Community: District 3

#### Trend 1: Decline in Registered Business Locations
1. **Clear Pattern**: There is a downward trend over the past decade in the number of registered business locations in District 3. As of December 2024, the count is 1,726, which is 37% below the 10-year average of 2,756.
2. **Memorable Example**: In December 2024, the business count fell to 1,726, a 12% decline from the previous year's total of 1,953.
3. **Chart Links**: 
   - [Count by Year Chart](../static/chart_2f86bf.png)
   - [Count by Year by NAICS Code Description Chart](../static/chart_8eb52f.png)
4. **Query URL**: [Registered Business Locations Query](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
5. **Compelling Chart**: The count by year chart is especially compelling, illustrating the consistent decline over the decade.

#### Trend 2: Increase in Police Incident Reports, but Decrease in 2024
1. **Clear Pattern**: Police incident reports initially increased but saw a notable decrease in 2024. The total incident count in 2024 was 14,196, which is 24% below the 2023 total of 18,732.
2. **Memorable Example**: Property Crime had 6,501 incidents in 2024, 37% below the 2023 total of 10,265.
3. **Chart Links**:
   - [Incident Count by Year Chart](../static/chart_bbb06a.png)
   - [Incident Count by Year by Grouped Category Chart](../static/chart_e76f4c.png)
4. **Query URL**: [Police Incident Reports Query](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+...+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category+...)
5. **Compelling Chart**: The incident count by year by grouped category chart highlights significant declines in specific crime categories.

#### Trend 3: Consistent Fire Violations with Significant Changes in Status
1. **Clear Pattern**: Fire violation counts have been consistent, though the status of these violations has seen changes. In December 2024, the count was 1,113, 8% below the 10-period average of 1,213.
2. **Memorable Example**: Open violations increased by 1100% above the 10-period average in 2024.
3. **Chart Links**:
   - [Violation Count by Year Chart](../static/chart_e59fda.png)
   - [Violation Count by Status Chart](../static/chart_cf2e74.png)
4. **Query URL**: [Fire Violations Query](https://data.sfgov.org/resource/4zuq-2cbe.json?%24query=SELECT+violation_item_description%2C+status%2C+battalion%2C...+WHERE+violation_date+%3E%3D%272014-01-01%27...)
5. **Compelling Chart**: The violation count by status chart showcases the dramatic rise in open violations for 2024.

### Questions for Further Analysis:
- What are the potential reasons for the decline in business registrations in District 3?
- Which specific initiatives or changes in policy might have influenced the reported trends in police incidents?
- How have responses to fire violations been adjusted given the increase in open status reports?