### Significant Long-term Economic and Community Trends in San Francisco (2014-2024)

#### 1. Decrease in Building Permits
- **Pattern**: A consistent decline in the number of building permits issued over the past decade. In December 2024, the permit count was 25,279, which is 26% below the 10-year average of 34,274.
- **Example**: The total permit count for YTD 2024 is marginally above the YTD 2023 total by 1%. Yet, the estimated cost of construction for these permits was significantly down, suggestive of fewer large-scale projects.
- **Query URL**: [Building Permits City Wide](https://data.sfgov.org/resource/i98e-djp9.json?%24query=SELECT+permit_type_definition%2C+existing_use%2C+date_trunc_y%28permit_creation_date%29+as+year%2C+count%28%2A%29+as+permit_count%2C+sum%28estimated_cost%29+as+estimated_cost_sum%2C+sum%28proposed_units%29+as+proposed_units%2C+sum%28existing_units%29+as+existing_units%2C+proposed_construction_type%2C+status+WHERE+permit_creation_date+%3E%3D%272014-01-01%27+GROUP+BY+status%2Cpermit_type_definition%2C+existing_use%2C+year%2C+proposed_construction_type+LIMIT+5000+OFFSET+10000)
- **Charts**: 
  - Building Permits permit count by year: ![Chart](../static/chart_9b84ea.png)
  - Building Permits estimated cost sum by year: ![Chart](../static/chart_412b60.png)

#### 2. Vacancy Tax Filings
- **Pattern**: A decrease in vacancy tax filings, indicating either a resolution of vacancies or changes in reporting practices.
- **Example**: Count was 3,181 in December 2024, 14% below the 2023 average of 3,684.
- **Query URL**: [Vacancy Tax Filings](https://data.sfgov.org/resource/rzkk-54yv.json?%24query=SELECT+taxyear+as+year%2C+ParcelNumber%2C+FilerType%2C+Vacant%2C+analysis_neighborhood%2C+supervisor_district%2C+count%28%2A%29+as+item_count+WHERE+data_as_of%3E%3D%272014-01-01%27+GROUP+BY+year%2C+ParcelNumber%2C+FilerType%2C+Vacant%2C+analysis_neighborhood%2C+supervisor_district+LIMIT+5000+OFFSET+5000)
- **Charts**: 
  - Commercial Vacancy Tax count by year: ![Chart](../static/chart_fc56b0.png)

#### 3. Economic Activity in the Form of Fire Violations
- **Pattern**: The fluctuation in fire violation counts suggests ongoing changes in business operations and regulations compliance.
- **Example**: As of December 2024, total violation count was 4,364, 23% above the 10-year average but 12% below YTD 2023.
- **Query URL**: [Fire Violations City Wide](https://data.sfgov.org/resource/4zuq-2cbe.json?%24query=SELECT+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+date_trunc_y%28violation_date%29+AS+year%2C+COUNT%28%2A%29+AS+violation_count+WHERE+violation_date+%3E%3D%272014-01-01%27+GROUP+BY+violation_item_description%2C+status%2C+battalion%2C+station%2C+neighborhood_district%2C+supervisor_district%2C+zipcode%2C+year+ORDER+BY+year+violation item description&LIMIT=5000&OFFSET=20000)
- **Charts**: 
  - Fire Violations violation count by year: ![Chart](../static/chart_a23522.png)

Each of these trends illustrates shifts in San Francisco's economic and community landscape over the years, supported by concrete data.

**Questions for Analysts**:
1. Are the declines in building permit values indicative of economic downturns, or are there systemic factors such as regulatory changes affecting permit applications?
2. What specific neighborhood-level actions might be driving the reductions or anomalies in vacancy tax filings?
3. What underlying strategies might fire violation trends suggest about safety compliance or changes in business activities over this period?