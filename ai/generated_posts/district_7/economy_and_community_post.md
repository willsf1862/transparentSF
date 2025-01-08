Here are some long-term trends and anomalies in District 7 of San Francisco, focusing on economy and community data from 2014 to 2024:

### **Trend: Decline in Business Registrations**
1. **Overarching Pattern:**
   - Business registrations in District 7 have been consistently declining over a ten-year period.
   
2. **Specific Examples:**
   - YTD 2024 saw a total count of businesses at 574, marking a 5% decrease from YTD 2023. In December 2024 alone, the count was 574, 28% below the 10-year average of 800.

3. **Supporting Charts and Data:**
   - [Registered Business Locations - San Francisco count by year chart](../static/chart_a9968a.png)
   - [Registered Business Locations by NAIC code](../static/chart_72c73e.png)
   
4. **Query URL:**
   - [Business Locations District 7 Query](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

5. **Compelling Chart:**
   - [Business Locations Count by Supervisor District](../static/chart_c6d24c.png)

### **Trend: Decrease in Police Incident Reports**
1. **Overarching Pattern:**
   - A significant drop in police incident reports in District 7 from 2018 to 2024, with a noticeable decrease in property crimes.
   
2. **Specific Examples:**
   - Total incidents in 2024 numbered 4,770, a decline of 29% from 6,697 in 2023. Property crimes in 2024 were 2,613, 38% down from 4,183 in the previous year.

3. **Supporting Charts and Data:**
   - [Police Incident Reports Count by Year](../static/chart_48e9ae.png)
   - [Incident Count by Incident Category](../static/chart_7c7c34.png)
   
4. **Query URL:**
   - [Police Incident Reports District 7 Query](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+35000)

5. **Compelling Chart:**
   - [Incident Count by Year by Supervisor District](../static/chart_346fd7.png)

### Questions for Further Analysis:
1. What sectors, if any, are showing growth or resilience amidst the declining business registration trends?
2. Are there specific interventions or policy changes that correlate with the reduction in property crimes?
3. Is the decrease in incident reports consistent across all neighborhoods within District 7?

These trends and the associated data can help identify structural changes in District 7 and serve as a basis for in-depth exploration through discussions with city planners, local businesses, and law enforcement agencies.