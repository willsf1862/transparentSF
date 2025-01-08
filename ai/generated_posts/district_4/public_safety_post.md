Here are the significant long-term trends in public safety data for District 4 from 2014 to 2024, focusing on crime rates and specific incident types:

1. **Overall Crime Trends**  
   **Pattern:** Overall crime in District 4 has shown a consistent decrease over the past few years, reaching 13% below the 2023 total by the end of 2024.  
   **Example:** In 2024, there were 3,509 total incidents, indicating an 11% decrease from a six-year average of 3,922.  
   **Chart:** ![Overall Crime](../static/chart_13e17a.png)  
   **Query:** [Police Incident Reports](https://data.sfgov.org/resource/wg3w-h783.json?%24query=SELECT+Incident_Category%2C+Incident_Subcategory%2C+supervisor_district%2C+CASE+WHEN+Incident_Category+IN+%28%27Assault%27%2C+%27Homicide%27%2C+%27Rape%27%2C+%27Robbery%27%2C+%27Human+Trafficking+%28A%29%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking%2C+Commercial+Sex+Acts%27%2C+%27Human+Trafficking+%28B%29%2C+Involuntary+Servitude%27%2C+%27Offences+Against+The+Family+And+Children%27%2C+%27Weapons+Carrying+Etc%27%2C+%27Weapons+Offense%27%2C+%27Weapons+Offence%27%29+THEN+%27Violent+Crime%27+WHEN+Incident_Category+IN+%28%27Arson%27%2C+%27Burglary%27%2C+%27Forgery+And+Counterfeiting%27%2C+%27Fraud%27%2C+%27Larceny+Theft%27%2C+%27Motor+Vehicle+Theft%27%2C+%27Motor+Vehicle+Theft%3F%27%2C+%27Stolen+Property%27%2C+%27Vandalism%27%2C+%27Embezzlement%27%2C+%27Recovered+Vehicle%27%2C+%27Vehicle+Impounded%27%2C+%27Vehicle+Misplaced%27%29+THEN+%27Property+Crime%27+WHEN+Incident_Category+IN+%28%27Drug+Offense%27%2C+%27Drug+Violation%27%29+THEN+%27Drug+Crimes%27+ELSE+%27Other+Crimes%27+END+AS+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+date_trunc_y%28Report_Datetime%29+AS+year%2C+COUNT%28%2A%29+AS+incident_count+WHERE+Report_Datetime+%3E%3D%272014-01-01%27+GROUP+BY+supervisor_district%2C+grouped_category%2C+Report_Type_Description%2C+Police_District%2C+Incident_Category%2C+Incident_Subcategory%2C+year+ORDER+BY+year+LIMIT+5000+OFFSET+35000)  
   **Compelling Chart:** Overall decrease in crime categories in 2024, specifically in property crimes.

2. **Property Crime**  
   **Pattern:** Property crimes decreased significantly by 21% compared to 2023.  
   **Example:** The incidents classified as Larceny Theft showed a notable decrease of 28% from the previous year, whereas commercial burglaries surged by 167%.  
   **Chart:** ![Property Crime](../static/chart_71ea56.png)  
   **Compelling Chart:** Drop in property crime overall, yet a spike in commercial burglaries.

3. **Fire Violations**  
   **Pattern:** Fire violation counts fluctuated over the years. However, 2024 saw a 24% decrease in total violation counts compared to 2023, despite a temporary rise in violations referred to hearings by 475%.  
   **Example:** YTD violation counts in December 2024 ended at 63, 24% lower than the previous year.  
   **Chart:** ![Fire Violations](../static/chart_a6a971.png)  
   **Query:** [Fire Violations](https://data.sfgov.org/resource/4zuq-2cbe.json?%24query=SELECT+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+date_trunc_y%28violation_date%29+AS+year+WHERE+violation_date+%3E%3D%272014-01-01%27+GROUP+BY+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode+ORDER+BY+year%2C+violation_item_description+LIMIT+5000)  
   **Compelling Chart:** Violation trends and spikes in referrals to hearings.

4. **Traffic Crashes**  
   **Pattern:** Traffic-related fatalities have decreased over the years, maintaining a low count in 2024.  
   **Example:** In 2024, December ended with only 1 fatality, maintaining a downward trend from previous years.  
   **Chart:** ![Traffic Crashes](../static/chart_29ff4b.png)  
   **Query:** [Fatal Traffic Crashes](https://data.sfgov.org/resource/dau3-4s8f.json?%24query=SELECT+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district%2C+date_trunc_y%28collision_datetime%29+AS+year+WHERE+collision_datetime+%3E%3D%272014-01-01%27+GROUP+BY+collision_type%2C+street_type%2C+sex%2C+supervisor_district%2C+analysis_neighborhood%2C+police_district+ORDER+BY+year+LIMIT+5000)  
   **Compelling Chart:** Consistently low fatality counts on city streets.

5. **Calls for Service**  
   **Pattern:** There were noticeable decreases in law enforcement dispatched calls, with only minimal incidents in 2024.  
   **Example:** Only 1 call for the police agency was recorded in 2024.  
   **Chart:** ![Calls for Service](../static/chart_8b7c23.png)  
   **Query:** [Dispatched Calls for Service](https://data.sfgov.org/resource/gnap-fj3t.json?%24query=SELECT+agency%2C+call_type_final%2C+priority_final%2C+onview_flag%2C+disposition%2C+supervisor_district%2C+police_district%2C+date_trunc_y%28received_datetime%29+AS+year+ORDER+BY+year+LIMIT+5000)  
   **Compelling Chart:** Annual drastic reduction in reported calls.

These insights illustrate a general downward trend in various aspects of public safety within District 4, accompanied by specific anomalies that reflect broader shifts in public safety data.