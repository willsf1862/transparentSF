### District 9 Health and Social Services: Long-term Data Trends (2015-2024)

#### 1. Overarching Patterns

**Traffic Crashes and Fatalities Reduction**
- The total fatality count due to traffic crashes in District 9 has significantly decreased to 2 in 2024, reflecting a long-term downward trend of 75% from the previous year’s total of 8.
- Long-term analysis suggests a consistent decrease across various types of collisions, especially in motorcycle and pedestrian-related fatalities.

**Significant Decrease in Property Loss Due to Fire Incidents**
- The estimated property loss sum from fire incidents has declined dramatically. In December 2024, it was recorded at 3,034,535, which represents a 61% reduction compared to 2023's total of 7,720,712.

**Civilian Injuries in Fire Incidents**
- The total number of civilian injuries from fire incidents in District 9 increased by 80% in 2024 compared to 2023, totaling 9 injuries. This marks an upward anomaly as previous counts have remained relatively low.

#### 2. Specific Examples

**Noteworthy Decrease in Fatal Accidents by Type**
- For motorcycle versus motor vehicle scenarios, fatalities in 2024 matched the long-term average, demonstrating improved measures in traffic safety.
  
**Sharp Decline in Estimated Property Loss**
- Within fire incidents, estimated property losses associated with "Fires in Structures Other Than a Building" observed a significant reduction in costs, dropping from a 10-period average to virtually zero in 2024.

#### 3. Relevant Charts

- **Fatality Count by Year**: This chart depicts the significant reduction in fatality counts since 2014.
  - ![Traffic Fatalities](../static/chart_002fea.png)

- **Estimated Property Loss Due to Fire Incidents by Year**: This chart illustrates the large drop in property losses over the years.
  - ![Property Loss](../static/chart_09551b.png)

#### 4. Query URL for Original Data

- **Traffic Crashes**: 
  - [Traffic Crashes Resulting in Fatality Query URL](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+fatality_count+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

- **Fire Incidents**: 
  - [Fire Incidents Query URL](https://data.sfgov.org/resource/wr8u-xric.json?%24query=SELECT+date_trunc_y%28incident_date%29+AS+year%2C+count%28%2A%29+AS+fire_incident_count%2C+sum%28estimated_property_loss%29+AS+estimated_property_loss_sum%2C+sum%28estimated_contents_loss%29+AS+estimated_contents_loss_sum%2C+sum%28fire_fatalities%29+AS+fire_fatalities_sum%2C+sum%28fire_injuries%29+AS+fire_injuries_sum%2C+sum%28civilian_fatalities%29+AS+civilian_fatalities_sum%2C+sum%28civilian_injuries%29+AS+civilian_injuries_sum%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+WHERE+incident_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+ORDER+BY+year+LIMIT+5000+OFFSET+140000)

These findings highlight changing patterns in public health and safety over a decade in District 9, providing insights into improved measures and emerging challenges.