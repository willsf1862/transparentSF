In analyzing long-term trends in San Francisco's health and social services data from 2014 to 2024, we will highlight significant patterns and anomalies. This includes trends such as drug-related deaths, service requests, emergency and non-emergency calls, overdose-related 911 calls, and substance use service data.

### Trend 1: Drug-Related Deaths and Overdose-Related 911 Calls

1. **Pattern:** There has been an overall decrease in unintentional drug overdose deaths from 810 in 2023 to 586 in 2024, marking a 28% decrease. In December 2024, the number of deaths is 17% below the four-year period average.

2. **Specific Example:** Overdose-related 911 calls decreased by 32% from 4,557 in 2023 to 3,085 in 2024, highlighting a decline in emergency demand for overdose incidents.

3. **Anomalies:** While overall calls decreased, there was a 5% increase in overdose-related 911 calls in December 2024 compared to the two-period average.

4. **Data Query URL:** [Drug Overdose Deaths](https://data.sfgov.org/resource/jxrr-bmra.json?%24query=SELECT+year%2C+SUM%28total_deaths%29+AS+death_count+WHERE+month_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year+ORDER+BY+year+LIMIT+5000+OFFSET+0)
   
5. **Charts:** [Overdose Deaths Chart](output/jxrr-bmra.json.html), [911 Calls Chart](output/ed3a-sn39.json.html)

### Trend 2: Substance Use Service Data

1. **Pattern:** Total substance use services increased by 109% as compared to the four-period average, reaching 161,045 in 2024. There was a 9% increase from 147,090 in 2023.

2. **Specific Example:** Naloxone Distribution services increased by 65% over the four-period average, reaching 154,464 services in 2024. Yearly, it increased by 10% from 139,828 in 2023.

3. **Anomalies:** No notable anomalies were detected in the data regarding substance use services between 2014 and 2024.

4. **Data Query URL:** [Substance Use Services](https://data.sfgov.org/resource/ubf6-e57x.json?%24query=SELECT+year%2C+sum%28metric_value%29+as+total_services%2C+service_category%2C+metric+WHERE+data_loaded_at+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+service_category%2C+metric+LIMIT+5000+OFFSET+0)
   
5. **Charts:** [Substance Use Services Chart](output/ubf6-e57x.json.html)

These trends are indicative of both positive changes in overdose treatment and reporting, as well as a need for continued focus on public health initiatives, particularly involving substance use treatments and emergency response adjustments. Further analysis should delve into regional distribution of these trends in San Francisco to identify localized needs or outcomes.