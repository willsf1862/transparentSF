### Health and Social Services Trends in District 3 (2014-2024)

Here's a factual summary of the long-term trends and specific anomalies in District 3 regarding health and social services:

#### 1. Law Enforcement Dispatched Calls
- **Overarching Pattern**: Call counts have been decreasing over the years, particularly in 2024, showing a 3% decrease from 2023.
- **Specific Example**: The total call count for police in 2024 was 52,981, which is 5% below the YTD 2023 total of 55,664.
- **Chart Link**: ![Law Enforcement Chart](../static/chart_d08e8b.png)
- **Query URL**: [Law Enforcement Calls](https://data.sfgov.org/resource/2zdj-bwza.json?%24query=SELECT+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+date_trunc_y%28received_datetime%29+AS+year+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime+%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+535000)

#### 2. Police Incident Reports
- **Overarching Pattern**: There has been a significant decrease in the number of incidents reported.
- **Specific Example**: In 2024, the total incident count was 14,196, which is 24% below the 2023 total of 18,732.
- **Chart Link**: ![Police Incident Chart](../static/chart_bbb06a.png)
- **Query URL**: [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)

### Questions for Analysts
1. What are the specific factors that contributed to the decrease in law enforcement dispatched calls and police incidents in 2024?
2. How can these trends be compared to broader citywide patterns, if any, to understand the implications for local policy-making?

The analysis reveals significant declines in emergency responses and incident reports in District 3 for 2024. These data-driven insights can serve as a basis for deeper investigations into the operational aspects of health and social services in this area.