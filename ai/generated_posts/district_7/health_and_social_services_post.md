Here are significant long-term trends related to health and social services in District 7 over the past 5 to 10 years, with a focus on 2024 comparisons:

### 1. Traffic Crashes Resulting in Fatalities
- **Trend:** A notable increase in traffic crashes resulting in fatalities in 2024.
- **Data Insight:** The fatality count in 2024 was 7, significantly higher than the historical annual average of 2.
- **Specific Example:** Pedestrian vs Motor Vehicle collisions led to 4 fatalities in December 2024, a 233% increase compared to the 10-period average.
- **Query URL:** [Traffic Crashes Resulting in Fatality](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+fatality_count+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)
- **Compelling Chart:** 
  ![Traffic Crashes Resulting in Fatality](../static/chart_d60d1f.png)

### 2. Business Location Registrations 
- **Trend:** A decline in business registrations in 2024 in District 7.
- **Data Insight:** Business locations in District 7 were down to 574 in 2024, marking a significant drop from 800, the 10-period average.
- **Specific Example:** The West of Twin Peaks neighborhood saw a 30.5% decrease in business registrations.
- **Query URL:** [Registered Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
- **Compelling Chart:** 
  ![Registered Business Locations](../static/chart_a9968a.png)

### 3. Policing and Public Safety
- **Trend:** A substantial reduction in incident reports in District 7 in 2024.
- **Data Insight:** The total incident count stood at 4,770 in 2024, down 29% from the previous year of 6,697.
- **Specific Example:** Larceny Theft incidents were significantly lower at 1,468, a 41% decrease from 2023.
- **Query URL:** [Police Department Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+35000)
- **Compelling Chart:** 
  ![Police Department Incident Reports](../static/chart_48e9ae.png)

These trends allow for focused investigations into traffic safety measures, business environment changes, and public safety tactics within District 7, enhancing understanding based on objective data.