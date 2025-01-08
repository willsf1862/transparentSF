### 1. Long-Term Economic Trend: Decline in Registered Business Locations in District 10

#### Pattern
The overarching trend in District 10 has been a steady decline in registered business locations since 2014. The count of registered businesses in District 10 was consistently shrinking over the years, with a more notable decrease observed from 2017 onwards.

- **Query URL:**
  [Business Locations District 10](https://data.sfgov.org/resource/g8m3-pdis.json?%24query=SELECT+date_trunc_y%28location_start_date%29+AS+year%2C+count%28%2A%29+as+item_count%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+WHERE+location_start_date+%3E%3D%272014-01-01%27+GROUP+BY+year%2C+naic_code_description%2C+supervisor_district%2C+neighborhoods_analysis_boundaries+LIMIT+5000+OFFSET+5000)

#### Specific Illustration
- As of the end of 2024, the total count of registered businesses was 854, down 15% from the previous year's total of 999.
- Comparatively, in December 2014, the business count was 1,308. Thus, over ten years, there was a stark decrease of over 34.7%.

- **Chart Links:**
  - [Business Locations - Count by Year](../static/chart_1de4dc.png)
  - [Count by Year by NAICS Code](../static/chart_049ecb.png)

#### Anomaly and Visual Representation
- The Bayview Hunters Point area saw a 31.3% decrease in registered businesses compared to the average over the prior decade. The starkest drop is represented in the chart.
- **Compelling Chart:**
  [Bayview Hunters Point Anomaly Chart](../static/chart_f95d1249.png)

### Next Steps
Analysts should focus on understanding specific factors contributing to the decline and assess which sectors are most affected. Are some sectors more resilient? What distinguishes the resilient sectors?