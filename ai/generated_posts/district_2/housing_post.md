### Long-term Housing Trends in District 2

#### Trend 1: Fire Incidents with Significant Property Loss
1. **Pattern**: The incidence of fire incidents in multifamily and single-family dwellings has shown varied patterns over the past decade, leading to significant economic consequences in terms of property loss.
2. **Example**: In 2024, estimated property loss for 1 or 2 family dwellings increased dramatically to $9,448,000, marking a 545.8% increase compared to the prior years' averages. 
3. **Charts**:
   - Fire incidents by year for multifamily dwellings [Chart](../static/chart_f1c526.png)
   - Estimated property loss by year [Chart](../static/chart_ad6409.png)
4. **Query URL**: [Fire Incident Reports Query](https://data.sfgov.org/resource/wr8u-xric.json?$query=SELECT+date_trunc_y%28incident_date%29+AS+year,...)
5. **Anomaly Chart**: Estimated property loss for 1 or 2 family dwellings [Chart](../static/chart_ad6409.png)

#### Trend 2: Increasing Notices of Violation in Housing
1. **Pattern**: There has been an increasing trend in the number of Notices of Violation (NOV) in housing, especially related to active cases.
2. **Example**: The number of active NOVs jumped to 404 in 2024, representing a staggering 359.1% increase from the previous averages.
3. **Charts**:
   - Notices of Violation count by year [Chart](../static/chart_41441c.png)
   - NOV category description by year [Chart](../static/chart_44f5cc.png)
4. **Query URL**: [Notices of Violation Query](https://data.sfgov.org/resource/nbtm-fbw5.json?$query=SELECT+date_trunc_y%28date_filed%29+AS+year,...)
5. **Anomaly Chart**: Active NOV count [Chart](../static/chart_94b4850c.png)

I will generate a report now for these findings.The report "District 2 Housing: Fires, Losses, and Violations Rising" has been generated. You can access it [here](https://anomaloussf.replit.app/p/a32b7927-7576-4820-b00a-018f9fc27245/). This report details the significant long-term trends in housing for District 2, highlighting fire incidents leading to substantial property loss and the rise in Notices of Violation, particularly active ones. Charts and data sources are provided to support the findings.