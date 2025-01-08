### District 2 Public Safety Trends (2014-2024)

#### 1. Decline in Calls for Service
- **Pattern:** From 2014 to 2024, District 2 has seen a noticeable decline in the total number of calls for service. By December 2024, call counts were 8% below the YTD 2023 figures. 
- **Example:** A specific decline can be observed in police calls dropping by 19% from YTD 2023, while calls from the Municipal Transportation Agency increased by 12%.
- **Charts:**
  - [Calls for Service Count by Year](../static/chart_93fb62.png)
  - [Calls by Agency](../static/chart_adcc63.png)
- **Query URL:** [View Data](https://data.sfgov.org/resource/gnap-fj3t.json?%24query=SELECT+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+date_trunc_y%28received_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime+%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

#### 2. Decrease in Police Incident Reports
- **Pattern:** Incident reports filed dropped significantly by 26% in 2024 compared to 2023, with a steady decline observed over the years.
- **Example:** A sharp decline in property crimes with 3,739 cases in 2024, which is 33% lower than the previous year.
- **Charts:**
  - [Incident Count by Year](../static/chart_c4c211.png)
  - [Incident Count by Category](../static/chart_151ef3.png)
- **Query URL:** [View Data](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+35000)

#### 3. Stable Fatal Traffic Crashes
- **Pattern:** Fatal traffic crashes remain fairly stable with minor fluctuations over 2014-2024.
- **Example:** In 2024, fatal crashes were consistently around previous averages; this is evident from recent data showing 3 fatalities which align with the historical average.
- **Charts:**
  - [Fatality Count by Year](../static/chart_63cbc4.png)
- **Query URL:** [View Data](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+fatality_count+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

#### Questions for Further Analysis:
1. What factors contributed to the significant reduction in police calls and incident reports? 
2. How have changes within the police and municipal transportation agencies affected response and resolution of calls?
3. Are there any emerging patterns in the types of collisions or street types involved in fatal traffic crashes that necessitate closer examination?

Each trend is defined clearly based on turns and examples. Let these objective data guide further investigations and analyses in public safety.