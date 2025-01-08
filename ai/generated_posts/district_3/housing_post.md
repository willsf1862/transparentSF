### Housing Trends in District 3, San Francisco

#### Long-Term Trends (2014-2024)
1. **Notices of Violation:**
   - Over the 10 years, the number of notices of violation issued by the Department of Building Inspection fluctuated but remained significant, influencing housing conditions in the area.
   - **Query URL:** [Notices of Violation](https://data.sfgov.org/resource/nbtm-fbw5.json?%24query=SELECT+date_trunc_y%28date_filed%29+AS+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode%2C+COUNT%28%2A%29+AS+item_count+WHERE+date_filed+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+status%2C+nov_category_description%2C+receiving_division%2C+assigned_division%2C+supervisor_district%2C+zipcode+ORDER+BY+year+LIMIT+5000+OFFSET+10000)
   - **Chart:** ![Notices of Violation by Year](../static/chart_f950c9.png)

   - **Details:** In 2024, the total count of notices increased by 33% over 2023, highlighting ongoing issues in housing conditions. 
   - **Specifics:** The 'active' notices increased by 161% compared to 2023, reflecting more ongoing regulatory issues. See [Chart](../static/chart_7f3892.png).

2. **Business Locations:**
   - This dataset reflected long-term shifts in business types, which can directly affect housing demand and neighborhood character.
   - **Query URL:** [Business Locations](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)
   - **Chart:** ![Business Locations by Year](../static/chart_2f86bf.png)

   - **Details:** By December 2024, there was a significant decline of 37% below the 10-year average in business locations, linked closely with a decrease in new business openings.
   - **Specifics:** Businesses in Financial, Professional Services, and Real Estate sectors saw notable declines, indicating potential economic shifts. See [Chart](../static/chart_8eb52f.png).

### Anomalies and Specific Examples
- **Notices of Violation Anomalies:**
  - 'Active' notices soared by over 500% compared to historical averages signaling widespread regulatory incompliances or active efforts for housing safety (Chart: ![Active Notices](../static/chart_a216e612.png)).
  
- **Business Sector Drop:**
  - Information sector saw a 56% drop below historical averages in business counts, a shift that could impact local employment and housing needs (Chart: ![Information Sector](../static/chart_b8f067fd.png)).

### Conclusion
The data shows significant trends in regulatory measures and business dynamics which impact housing in District 3. The notable spikes in active violations and declines in sector-specific business locations suggest significant influences on the community's living conditions and economic framework. Further scrutiny can reveal deeper connections to resident experiences and forecast future developments.

### Analyst Questions
1. What are the underlying reasons for the increase in active notices of violation?
2. How do shifts in business sector dynamics correlate with housing trends and demands in District 3?

This detailed analysis reflects concrete changes in District 3, forecasting possible implications on the housing landscape based on precise data points.