### Long-Term and Recent Trends in District 8: Economy and Community

#### 1. Business Locations in District 8
- **Overarching Pattern**: There is a long-term decline in the registration of business locations in District 8. The count has decreased from 1,425 in 2014 to 898 in 2024, marking a significant reduction over a decade.
- **Specific Examples**: For example, the number of registered business locations in Noe Valley decreased by 38.7% compared to the ten-year average, with only 190 count in 2024. Construction industry registrations dropped significantly, as did the arts, entertainment, and recreation sectors in this district.
- **Charts**:
  - ![Business Locations by Year](../static/chart_37bd9c.png)
  - ![Business Locations by NAIC Code](../static/chart_cd8202.png)
  - ![Business Locations by Neighborhood](../static/chart_923d5c.png)
- **Query URL**: [Business Locations District 8 Query](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
- **Compelling Chart**: [Overall Business Decrease](../static/chart_79c05168.png)

#### 2. Police Incidents in District 8
- **Overarching Pattern**: Over recent years, police incidents have decreased in District 8. The total incident count has reduced from 9,146 in 2018 to 6,984 in 2024.
- **Specific Examples**: “Larceny - From Vehicle” incidents showed a notable anomaly with a reduction of 573 incidents, 42.6% below the comparison mean. Furthermore, burglary incidents have decreased consistently over several years.
- **Charts**:
  - ![Incident Count by Year](../static/chart_54814b.png)
  - ![Incident Subcategory Details](../static/chart_3afb06.png)
  - ![Incidents by Police District](../static/chart_f294dd.png)
- **Query URL**: [Police Incidents District 8 Query](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+T)
- **Compelling Chart**: [Larceny from Vehicle Reduction](../static/chart_b191eaac.png)

Both the reduction in business registrations and the decrease in police incidents provide an overview of shifts occurring in District 8 that reflect both economic downturns and possible enhancements in public safety and order. These trends illustrate critical aspects of economic and community dynamics in San Francisco.