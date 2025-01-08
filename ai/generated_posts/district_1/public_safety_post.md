# Public Safety Trends in District 1, San Francisco

## Trend 1: Decline in Police Department Incidents
### Pattern
There has been a significant long-term decrease in police department incidents in District 1. In 2024, the total incident count was 3,902, a 30% decline from 2023, and 33% below the six-year average of 5,830.

### Specific Example
Larceny Theft incidents fell by 50% in 2024 compared to 2023, with a count of 1,057 incidents, significantly below the previous year's total of 2,096.

### Charts
- [Incident Count by Year](../static/chart_260457.png)
- [Incident Count by Year by Category](../static/chart_86f2d8.png)

### Query URL
[Police Department Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+%2C+grouped_category+LIMIT+5000+OFFSET+35000)

### Most Compelling Chart
[Larceny Theft Incident Count](../static/chart_5eca3d0c.png)

## Trend 2: Increase in Specific Fire Incident Types
### Pattern
While general trends in fire incidents show periodic fluctuations, specific types like "700 False alarm or false call, other" have shown sharp increases, with incidents 134% above the 10-year average in 2024.

### Specific Example
"700 False alarm or false call, other" incident count reached 489 in 2024, which significantly overshot the historical mean of 209.

### Charts
- [Fire Incident Count by Year](../static/chart_3b318b.png)
- [False Alarm Incident Trend](../static/chart_9876d8ff.png)

### Query URL
[Fire Incident Reports](https://data.sfgov.org/resource/wr8u-xric.json?%24query=SELECT+date_trunc_y%28incident_date%29+AS+year%2C+count%28%2A%29+AS+fire_incident_count%2C+sum%28estimated_property_loss%29+AS+estimated_property_loss_sum%2C+sum%28estimated_contents_loss%29+AS+estimated_contents_loss_sum%2C+sum%28fire_fatalities%29+AS+fire_fatalities_sum%2C+sum%28fire_injuries%29+AS+fire_injuries_sum%2C+sum%28civilian_fatalities%29+AS+civilian_fatalities_sum%2C+sum%28civilian_injuries%29+AS+civilian_injuries_sum%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+WHERE+incident_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+ORDER+BY+year+LIMIT+5000+OFFSET+0)

### Most Compelling Chart
[700 False Alarm Incident Chart](../static/chart_9876d8ff.png)