### Trend 1: Decrease in Building Permits and Related Indicators

1. **Overarching Pattern:**  
   There has been a marked decrease in building permits issued in San Francisco for 2024 compared to historical averages. Specific indicators such as permit counts, estimated costs, and the number of proposed units show consistent downward trends.

2. **Specific Examples:**  
   - The number of building permits in 2024 was 25,279, which is 26% below the 10-year average of 34,274.  
   - Estimated costs for constructions in 2024 amounted to $2,334,988,807, 57% below the 10-year average.
   - Proposed units in 2024 totalled 277,438, which is 37% below the comparative average over the decade.

3. **Relevant Charts:**  
   - **[Permit Count by Year](../static/chart_9b84ea.png):** Shows a consistent decrease over time.
   - **[Estimated Cost Sum by Year](../static/chart_412b60.png):** Visualizes the reduction in cost.
   - **[Proposed Units by Year](../static/chart_17969f.png):** Illustrates the decline in proposed units.

4. **Query URL:**  
   [Building Permits Query URL](https://data.sfgov.org/resource/i98e-djp9.json?%24query=SELECT+permit_type_definition%2C+existing_use%2C+date_trunc_y%28permit_creation_date%29+as+year%2C+count%28%2A%29+as+permit_count%2C+sum%28estimated_cost%29+as+estimated_cost_sum%2C+sum%28proposed_units%29+as+proposed_units%2C+sum%28existing_units%29+as+existing_units%2C+proposed_construction_type%2C+status+WHERE+permit_creation_date+%3E%3D%272014-01-01%27+GROUP+BY+status%2Cpermit_type_definition%2C+existing_use%2C+year%2C+proposed_construction_type+LIMIT+5000+OFFSET+10000)

5. **Compelling Chart:**  
   - **[Permit Count by Year](../static/chart_9b84ea.png)**: This chart succinctly captures the downward trajectory of permit issuance.


### Trend 2: Changes in Commercial Vacancy and Response to Tax Filings

1. **Overarching Pattern:**  
   The commercial vacancy tax data for 2024 indicates changes in filing responses and vacancy reports, reflecting a shifting landscape in commercial real estate.

2. **Specific Examples:**  
   - In December 2023, the vacancy count was 3,181, which is 14% below the previous period's average.
   - Specific Parcel Numbers such as 1537-049 showed a 600% deviation from its usual average in December.

3. **Relevant Charts:**  
   - **[Vacancy Count by Year](../static/chart_fc56b0.png):** Displays the general decline in responses.
   - **[Vacancy Count by ParcelNumber](../static/chart_ca1f9a.png):** Highlights anomalies by specific parcels.

4. **Query URL:**  
   [Commercial Vacancy Tax Query URL](https://data.sfgov.org/resource/rzkk-54yv.json?%24query=SELECT+taxyear+as+year%2C+ParcelNumber%2C+FilerType%2C+Vacant%2C+analysis_neighborhood%2C+supervisor_district%2C+count%28%2A%29+as+item_count+WHERE+data_as_of%3E%3D%272014-01-01%27+GROUP+BY+year%2C+ParcelNumber%2C+FilerType%2C+Vacant%2C+analysis_neighborhood%2C+supervisor_district+LIMIT+5000+OFFSET+5000)

5. **Compelling Chart:**  
   - **[Vacancy Count by Year](../static/chart_fc56b0.png)**: Illustrates overall declines and is supported by marked parcel variances.

These trends reflect broader structural changes in San Francisco's housing and commercial real estate sectors as inferred from factual data, both longitudinal and current year-specific.