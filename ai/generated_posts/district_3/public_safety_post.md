### Long-term Trends in Public Safety in District 3

#### Trend 1: Decrease in Calls for Service
1. **Pattern:** Law enforcement dispatched calls across District 3 have shown a significant long-term decrease over the past decade.
2. **Examples Illustrating the Trend:**
   - The total call count in 2024 was 66,552, which is 19% below the 10-year average of 82,205.
   - The Police Department received 52,981 calls in 2024, a substantial 28.8% decrease compared to the decade average.
   
   ![Chart](../static/chart_d08e8b.png)
   - [Query URL: Law Enforcement Dispatched Calls](https://data.sfgov.org/resource/2zdj-bwza.json?$query=SELECT+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+date_trunc_y%28received_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime+%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+535000)
   - Compelling Chart: Call count by year showcases substantial decline.

#### Trend 2: Decrease in Property Crimes
1. **Pattern:** Property crimes in District 3 have substantially decreased over the last decade.
2. **Examples Illustrating the Trend:**
   - Reports of Larceny Theft dropped to 4,205 in 2024, which is 47.4% below the 6-year average of 7,998.
   
   ![Chart](../static/chart_111de9c6.png)
   - [Query URL: Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?$query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)
   - Compelling Chart: Incident count by year (Larceny Theft) visibly falling.

#### Trend 3: Fluctuations in Fire Incidents
1. **Pattern:** Fire incidents fluctuate but show a general rising trend for some specific types and locations.
2. **Examples Illustrating the Trend:**
   - Fire incidents at multifamily dwellings increased to 1,478 in 2024, 3% above 2023 but highlighting a percentage increase over the decade.
   - Estimated property losses in 2024 were 41% lower than in 2023, indicating fewer significant damages.
   
   ![Chart](../static/chart_82ddbd.png)
   - [Query URL: Fire Incident Reports](https://data.sfgov.org/resource/wr8u-xric.json?$query=SELECT+date_trunc_y%28incident_date%29+AS+year%2C+count%28%2A%29+AS+fire_incident_count%2C+sum%28estimated_property_loss%29+AS+estimated_property_loss_sum%2C+sum%28estimated_contents_loss%29+AS+estimated_contents_loss_sum%2C+sum%28fire_fatalities%29+AS+fire_fatalities_sum%2C+sum%28fire_injuries%29+AS+fire_injuries_sum%2C+sum%28civilian_fatalities%29+AS+civilian_fatalities_sum%2C+sum%28civilian_injuries%29+AS+civilian_injuries_sum%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+WHERE+incident_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+ORDER+BY+year+LIMIT+5000+OFFSET+140000)
   - Compelling Chart: Property loss chart highlights fluctuation in figures.

##### Analyst Questions:
- What factors contribute to the fluctuations in call counts for different agencies in the district?
- Can the decrease in property crimes be associated with specific prevention programs or resource allocations?
- What structural changes in multifamily dwellings lead to an increase in reported fire incidents?