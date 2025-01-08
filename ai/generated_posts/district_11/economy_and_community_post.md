**Trend 1: Decline in Business Locations**

1. **Overarching Pattern:** Over the past ten years, District 11 has experienced a decline in registered business locations, with a notable decrease in 2024 compared to prior years.
2. **Specific Examples:**
   - As of December, YTD 2024 counted 468 business locations, representing an 18% decrease from 571 in YTD 2023.
   - The decline is broad-based across sectors but most pronounced in retail trade, accommodations, and food services.
3. **Charts and Links:**
   - Registered Business Locations Count by Year
   - ![Chart](../static/chart_d358ab.png)
   - ![Chart](../static/chart_133cd8.png)
4. **Query URL:** [Business Locations Query](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

**Trend 2: Police Incident Count Decline with Specific Anomalies**

1. **Overarching Pattern:** The total incident count in District 11 has been decreasing, with a significant decline in 2024. However, some categories show anomalies compared to previous averages.
2. **Specific Examples:**
   - The 2024 total incident count of 5,058 is a 13% decrease from 2023.
   - Notable anomalies include increases in certain categories like sex offenses, which rose 125% above the previous average.
3. **Charts and Links:**
   - Incident Count by Year
   - ![Chart](../static/chart_1bf307.png)
   - ![Chart](../static/chart_c38dbbd4.png) for sex offense anomaly
4. **Query URL:** [Police Incident Query](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)

Further trends and specific insights can be expanded or explored by diving into fire incident reports, law enforcement dispatches, and their implications on the locality. Each data set offers potential leads for deeper inquiry into factors influencing the vibrant yet fluid socioeconomic terrain of District 11.