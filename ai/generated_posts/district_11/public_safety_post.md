Here are some notable trends in public safety data for San Francisco District 11 over the past decade, with a focus on the data from 2024. 

### 1. Crime-related Calls for Service
- **Long-term Trend**: The number of calls for service in District 11 has seen fluctuations over the years but overall shows a significant decline.
- **2024 Observation**: In December, the call_count was 1, reflecting no major change compared to historical averages.
- **Query URL**: [View Data](https://data.sfgov.org/resource/gnap-fj3t.json?%24query=SELECT+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+date_trunc_y%28received_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)
- **Chart**: [Service Calls Chart](../static/chart_4d0464.png)

### 2. Police Incident Reports
- **Long-term Trend**: Police incident reports in District 11 have generally decreased over the decade.
- **2024 Observation**: The total number of incidents was 5,058, a 13% decline from 2023.
    - Significant declines were observed in property crimes, down to 2,367 incidents, a decrease of 22%.
- **Query URL**: [View Data](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+%2C+grouped_category+LIMIT+5000+OFFSET+35000)
- **Chart**: [Incident Reports Chart](../static/chart_1bf307.png)

### 3. Fatal Traffic Crashes
- **Long-term Trend**: Fatal traffic crashes have been relatively stable but small in number each year.
- **2024 Observation**: The year ended with 2 fatalities, matching typical annual variations.
- **Query URL**: [View Data](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year+%2C+COUNT%28%2A%29+AS+fatality_count+WHERE+collision_datetime%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)
- **Chart**: [Fatal Crashes Chart](../static/chart_86adf4.png)

These trends reflect complex dynamics in public safety for District 11, with clear variations noticed over time. They also illustrate changes in citizen interactions with law enforcement and various public safety challenges in a rapidly evolving urban environment.