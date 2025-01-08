### Significant Trends in District 8: Health and Social Services 2024

#### Overview of Long-Term Trends

**1. Calls for Service Trends:**
- **Pattern:** Calls for service in District 8 show a consistent pattern. Dispatched calls have even outs over the period with a notable decrease in recent calls.
- **Example:** In 2024, the total call count stands at 39,742, which is 1% above the YTD 2023 total of 39,221.
- **Charts and Data Links:**
  - Query URL: [Call Service Query](https://data.sfgov.org/resource/gnap-fj3t.json?%24query=SELECT+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+date_trunc_y%28received_datetime%29+AS+year%2C+COUNT%28%2A%29+AS+call_count+WHERE+received_datetime+%3E%3D%272014-01-01%27+GROUP+BY+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)

**2. Police Incident Reports:**
- **Pattern:** Incident reports are on a slight decline, most notably in property crime.
- **Example:** In 2024, there were 6,984 reported incidents, an 8% decline from the previous year.
- **Charts and Data Links:**
  - Query URL: [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)

**3. Fire Incident Reports:**
- **Pattern:** Fire incidents have shown a relative consistency with minor fluctuations in counts.
- **Example:** While fire incident counts were 2,415 in 2024, which is 7% below the previous year's total of 2,595, estimated property loss increased by 7%.
- **Charts and Data Links:**
  - Query URL: [Fire Incident Reports](https://data.sfgov.org/resource/wr8u-xric.json?%24query=SELECT+date_trunc_y%28incident_date%29+AS+year%2C+count%28%2A%29+AS+fire_incident_count%2C+sum%28estimated_property_loss%29+AS+estimated_property_loss_sum%2C+sum%28estimated_contents_loss%29+AS+estimated_contents_loss_sum%2C+sum%28fire_fatalities%29+AS+fire_fatalities_sum%2C+sum%28fire_injuries%29+AS+fire_injuries_sum%2C+sum%28civilian_fatalities%29+AS+civilian_fatalities_sum%2C+sum%28civilian_injuries%29+AS+civilian_injuries_sum%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+WHERE+incident_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+primary_situation%2C+property_use%2C+structure_type%2C+supervisor_district%2C+neighborhood_district+ORDER+BY+year+LIMIT+5000+OFFSET+140000)

Each trend paints a part of the health and social services landscape in District 8, San Francisco, indicating potential focal points for resource allocation and community intervention. The dry numbers invite more descriptive analysis to address these different issues comprehensively, further inquiries to analysts would focus on the causes behind the changes.