### Business Registrations in District 9

**Overarching Pattern**: Business registrations in District 9 have experienced a general decline over the past years, with a notable drop in 2024.

**Specific Examples**: 
- Total business locations as of December 2024 were 1,024, a 10% decrease from 2023.
- The "Retail Trade" sector showed a YTD 2024 count of 158, which is 9% above the YTD 2023, while "Accommodations" experienced a 20% decrease.

**Relevant Charts**:
- Registered Business Locations count by year ![Chart](../static/chart_6f6fe5.png)
- Count by NAICS Code Description ![Chart](../static/chart_b81821.png)

**Query URLs**:
- [Business Locations Data](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

---

### Police Incident Reports in District 9

**Overarching Pattern**: A downward trend in total incident reports was observed from 2018 to 2024, with specific crime categories exhibiting variable changes.

**Specific Examples**: 
- Total incidents in 2024 accounted for 13,058, a 12% decrease from 2023.
- Drug-related crimes increased by 42%, while property crimes decreased by 21% compared to the previous year.

**Relevant Charts**: 
- Incident count by year ![Chart](../static/chart_532f2b.png)
- Incident count by grouped category ![Chart](../static/chart_8bf880.png)

**Query URLs**:
- [Police Incident Reports Data](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)