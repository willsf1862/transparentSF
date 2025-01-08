### Business Trends in District 5

1. **Long-term Business Decline**: 
   Over the last decade, there has been a decline in registered business locations in District 5, with a significant drop by 31% as of December 2024 compared to the 10-year average.

   - **Specific Example**: In December 2024, the total number of registered business locations was 791, which is 9% below the December total for 2023 of 869.
   
   - **Query URL**: [Business Locations Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   
   - **Chart**: ![Business Locations](../static/chart_8f9206.png)

2. **Shift in Business Types**:
   There has been a noticeable shift in the types of businesses operating in District 5, with food services observing an increase in contrast to other NAICS groups.

   - **Specific Example**: Throughout 2024, Food Services reported a 29% increase from the previous year, contrasting starkly with a 62% drop in 'NA' category businesses.
   
   - **Query URL**: Included within the previously provided [Business Locations Data URL](https://data.sfgov.org/resource/g8m3-pdis.json)
   
   - **Chart**: ![NAICS Business Types](../static/chart_d0256b.png)

### Community Trends in District 5

1. **Police Incident Reports**: 
   There has been a decline in total police incident reports over the years in District 5. The total incident count in 2024 was 17,773, showing a reduction compared to the previous year's figure of 20,629.

   - **Specific Example**: Property Crime incidents in 2024 witnessed a substantial drop, standing 29% below the average. Conversely, Drug Crimes increased slightly above average levels.
   
   - **Query URL**: [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)

   - **Chart**: ![District 5 Incident Reports](../static/chart_454386.png)

These trends reflect both challenges and transformations in the economy and community aspects of District 5, impacting local business strategies and policing dynamics.  

Questions:
- What are the specific causes behind the decline in business registrations?
- Are any new local policies impacting business growth and economic activity in the district?
- How are shifts in crime types impacting local community dynamics and public safety efforts in District 5?