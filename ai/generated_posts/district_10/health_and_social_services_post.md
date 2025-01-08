### Significant Trends in District 10's Health and Social Services (2024)

#### Trend 1: Calls for Service
1. **Pattern**: No significant anomalies in calls for service were detected from 2014 to 2024.
   - **Examples**: The dataset consistently shows no deviation.
   - **Query URL**: [Calls for Service](https://data.sfgov.org/resource/gnap-fj3t.json?%24query=SELECT+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+date_trunc_y%28received_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime+%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

#### Trend 2: Fire Violations
1. **Pattern**: The violation count in 2024 was 26% below the 2023 total, but December 2024 was 27% above the 10-year average.
   - **Examples**: Notable increases in referred to hearing violation counts denote an ongoing trend shift.
   - **Charts**: ![Fire Violations](../static/chart_d452447d.png)
   - **Query URL**: [Fire Violations](https://data.sfgov.org/resource/4zuq-2cbe.json?%24query=SELECT+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+date_trunc_y%28violation_date%29+AS+year+WHERE+violation_date+%3E%3D%272014-01-01%27+GROUP+BY+violation_item_description%2C+status+ORDER+BY+year)

#### Trend 3: Business Locations
1. **Pattern**: Registered business count decreased 15% from 2023 to 2024.
   - **Examples**: 31% decrease in December 2024 compared to the 10-year average shows significant business attrition.
   - **Charts**: ![Business Locations](../static/chart_1de4dc.png)
   - **Query URL**: [Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count+WHERE+location_start_date+%3E%3D%272014-01-01%27)

#### Trend 4: Police Incident Reports
1. **Pattern**: The incident count in 2024 decreased by 19% from 2023.
   - **Examples**: Larceny theft incidents down by 29% highlight a sector-wise crime drop.
   - **Charts**: ![Incident Reports](../static/chart_f76310.png)
   - **Query URL**: [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+supervisor_district%2C+date_trunc_y%28Report_Datetime%29+AS+year+WHERE+Report_Datetime+%3E%3D%272014-01-01%27)

These trends outline significant changes and continuity in District 10, assisting in the comprehension of long-term developments within the area's health and social services.