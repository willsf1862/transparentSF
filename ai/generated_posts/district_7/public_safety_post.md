### Public Safety Trends in District 7, San Francisco (2014-2024)

#### Overarching Trends
- **Public Safety Calls for Service**: Total calls showing consistent demand with spikes in specific service categories.
- **Traffic Crashes Resulting in Fatalities**: 2024 marked a notable increase in fatal traffic crashes compared to previous years.

---

#### Trend 1: Public Safety Calls for Service

##### Pattern
- The total number of police incident reports in District 7 decreased significantly over 2024, indicating a reduction in reported crimes.

##### Memorable Example
- **Property Crime**: The 2024 incident count was 2,613, marking a 38% reduction compared to the 2023 total of 4,183. 

![Chart](../static/chart_a01cbd60.png)

###### Query URL
[Police Department Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+35000)

---

#### Trend 2: Traffic Crashes Resulting in Fatalities

##### Pattern
- Fatalities from traffic crashes experienced a significant rise in 2024, primarily attributed to the increases in pedestrian vs. motor vehicle accidents.

##### Memorable Example
- **Fatality Count**: In 2024, there were 7 fatalities, 600% higher than in 2023, with most incidents occurring on city streets.

![Chart](../static/chart_d60d1f.png)

###### Query URL
[Fatal Traffic Crashes](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+fatality_count+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

---

Both of these trends, depicting a decrease in crime reports but an increase in traffic-related fatalities, should be examined further from operational and community protection perspectives.