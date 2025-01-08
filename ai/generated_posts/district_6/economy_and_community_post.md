### Trend 1: Decline in Registered Business Locations in District 6

#### Overarching Pattern:
The number of registered business locations in District 6 has shown a consistent decline from 2014 to 2024. Specifically, the count in 2024 was significantly lower compared to the 10-year average.

#### Specific Examples:
1. In December 2024, the total business count was 1,489, which is 44% below the 10-year average of 2,654 and 14% below the total for December 2023 (1,738).
2. The South of Market neighborhood saw a sharp decline with a 53% decrease from its 10-year average. 
   
#### Supporting Charts and Links:
- [Registered Business Locations - Count by Year](../static/chart_46c5d2.png)
- [Registered Business Locations - Count by Neighborhoods Analysis Boundaries](../static/chart_f7300f.png)
- [Registered Business Locations - Count by Supervisor District](../static/chart_fe52fd.png)

#### Query URL:
- [Economic Data Query URL](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

---

### Trend 2: Increase in Fire Violations in District 6

#### Overarching Pattern:
Fire violation counts in District 6 have experienced an upward trend, particularly noticeable in 2024 compared to previous years.

#### Specific Examples:
1. As of December 2024, the number of fire violations was 872, which is 7% above the total for December 2023 (815) and 4% above the 10-year average.
2. The "open" status category of violations saw a 144% increase from 2023 figures.

#### Supporting Charts and Links:
- [Fire Violations - Violation Count by Year](../static/chart_cb2998.png)
- [Fire Violations - Violation Count by Status](../static/chart_def186.png)

#### Query URL:
- [Fire Violations Data Query URL](https://data.sfgov.org/resource/4zuq-2cbe.json?%24query=SELECT+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+date_trunc_y%28violation_date%29+AS+year%2C+COUNT%28%2A%29+AS+violation_count+WHERE+violation_date+%3E%3D%272014-01-01%27+GROUP+BY+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+year+ORDER+BY+year%2C+violation_item_description+LIMIT+5000+OFFSET+20000)

---

### Trend 3: Reduction in Police Incident Reports

#### Overarching Pattern:
There has been a consistent reduction in police incident reports within District 6 over recent years, with a significant decrease in 2024.

#### Specific Examples:
1. In 2024, the total incident count was 17,472, which is 14% below the total for 2023 (20,325).
2. Property Crimes saw a noticeable reduction of 36% compared to the recent 6-period average.

#### Supporting Charts and Links:
- [Police Department Incident Reports - Incident Count by Year](../static/chart_cc0786.png)
- [Police Department Incident Reports - Incident Count by Grouped Category](../static/chart_d712d8.png)

#### Query URL:
- [Police Incident Reports Query URL](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year+%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year%2C+grouped_category+LIMIT+5000+OFFSET+35000)

The above trends paint a picture of evolving economic and community dynamics within District 6 of San Francisco, illustrating both challenges and changes over the years through 2024.