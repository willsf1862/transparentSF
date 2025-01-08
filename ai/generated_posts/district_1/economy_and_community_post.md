### Notable Long-term Trends in District 1: Economy and Community

#### Overview
Over the last decade, San Francisco's District 1 has experienced shifts in business registrations, employment dynamics, and community challenges that have notably impacted the local economy and community structure.

1. **Decline in Registered Business Locations**:
   San Francisco's District 1 has witnessed a marked decline in registered business locations. The number of businesses dropped by 28% in December 2024 compared to the average from the previous decade.
   - **Query URL**: [Business Locations Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   - **Charts**: 
     - [Chart of Annual Business Count](../static/chart_1e5057.png)
     - [Chart by NAICS Code](../static/chart_ad8777.png)
     - [Chart by Supervisor District](../static/chart_68e646.png)
   - **Memorable Example**: In the Professional, Scientific, and Technical Services industry, there were 60 businesses in December 2024, which is 20% below the 10-year average.

2. **Increased Fire Incidents but Less Financial Loss**:
   The count of fire incidents has risen by 6% in 2024 compared to 2023, with certain residential areas like multifamily dwellings seeing a 20% increase. However, the estimated property loss has decreased significantly.
   - **Query URL**: [Fire Incident Reports Data](https://data.sfgov.org/resource/wr8u-xric.json?%24query=SELECT+date_trunc_y%28incident_date%29+AS+year%2C+count%28%2A%29+AS+fire_incident_count%2C+sum%28estimated_property_loss%29+AS+estimated_property_loss_sum+...+WHERE+incident_date+%3E%3D%272014-01-01%27+GROUP+BY+year+...)
   - **Charts**:
     - [Fire Incident Count](../static/chart_3b318b.png)
     - [Estimated Property Loss](../static/chart_4ef262.png)
   - **Memorable Example**: In December 2024, fire incidents in multifamily dwellings were 78% above the 10-period average.

3. **Decrease in Police Incidents**:
   The overall number of police incidents in District 1 decreased significantly in 2024, falling 30% below the total for 2023.
   - **Query URL**: [Police Incident Reports Data](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%...+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district...)
   - **Charts**:
     - [Incident Count by Year](../static/chart_260457.png)
     - [Incident Count by Crime Type](../static/chart_86f2d8.png)
   - **Memorable Example**: Larceny Theft incidents decreased by 50% compared to 2023.

4. **Steady Low Fatal Traffic Crashes**:
   The number of fatal traffic crashes has remained low and stable with minor fluctuations over the years.
   - **Query URL**: [Fatal Traffic Crashes Data](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district...+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+...)
   - **Charts**:
     - [Fatality Count](../static/chart_f12305.png)

### Questions for Analysts:
- What factors have contributed to the decline in business registrations in District 1?
- Why has there been a reduction in estimated property loss despite an increase in fire incidents?
- What community or policing changes might be influencing the 30% decrease in police incidents?
- Can any additional data provide more insight into the variation in traffic fatalities, specifically street-level conditions or traffic management initiatives?

This analysis underscores significant economic and community shifts in District 1. Identifying the intricacies of these trends can offer comprehensive insights into the broader dynamics affecting San Francisco.