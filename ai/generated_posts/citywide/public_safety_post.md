### Trend: Traffic Crash Victims Decrease

#### Pattern:
Overall traffic crash victim counts have decreased. By December 2024, the total was 3,044, which is 20.9% lower than the ten-year average of 3,850.

#### Specific Examples:
1. Significant drop in "Broadside" type collisions by 19.1%.
2. Passenger victim roles showed a 32.2% decrease.
3. Victims with "Other Visible Injury" fell by 17.4%.

#### Supporting Charts:
- ![Traffic Crash Victims Chart](../static/chart_traffic_crash_victims.png)

**Query URL:** [Traffic Crash Data](https://data.sfgov.org/resource/nwes-mmgh.json?%24query=SELECT+collision_type%2C+COUNT%28%2A%29+AS+collision_count+WHERE+collision_date+%3E%3D%272014-01-01%27+GROUP+BY+collision_type)

---

### Trend: Decrease in Law Enforcement Dispatched Calls

#### Pattern:
Dispatched calls for service showed a significant decrease with nuanced agency-specific data points.

#### Specific Examples:
1. The "Other" agency experienced a 916.4% increase in anomaly.
2. Call types like "Suspicious Person" and "Traffic Stop" decreased by 36% and 53% respectively.

#### Supporting Charts:
- ![Calls for Service Chart](../static/chart_bd3df4.png)

**Query URL:** [Law Enforcement Dispatched Calls](https://data.sfgov.org/resource/2zdj-bwza.json?%24query=SELECT+agency%2C+call_type_final_desc%2C+disposition%2C+priority_final%2C+supervisor_district%2C+police_district%2C+analysis_neighborhood%2C+date_trunc_y%28received_datetime%29+AS+year+...)

---

### Trend: Decrease in 311 Service Requests

#### Pattern:
A substantial decrease in 311 service requests across all channels in 2024, with overall requests dropping by 63% when compared to 2023.

#### Specific Examples:
1. Mobile service requests fell by 63%.
2. Twitter requests declined by a drastic 76%.

#### Supporting Charts:
- ![311 Calls Chart](../static/chart_c3c11f.png)

**Query URL:** [311 Calls Data](https://data.sfgov.org/resource/mwjb-biik.json?%24query=SELECT+date_trunc_y%28month%29+AS+year%2C+sum%28calls_answered%29+...).

---

### Trend: Jail Booking Decline across All Age Groups

#### Pattern:
Continuous decline in overall jail bookings with consistent drops across all age groups. The total bookings in 2024 were 11,110, showing a 29% reduction from the ten-year average.

#### Specific Examples:
1. A noteworthy decline of 50% in bookings for the 18-24 age group.
2. A 17% decline was particularly notable for the 30-34 age group.

#### Supporting Charts:
- ![Jail Bookings Chart](../static/chart_989c38.png)

**Query URL:** [Jail Booking Data](https://data.sfgov.org/resource/pfbp-75pz.json?%24query=SELECT+age_group%2C+date_trunc_y%28month_and_year_of_booking%29+AS+year+...).

---

Each trend encapsulates significant changes affecting public safety metrics in San Francisco, underpinned by concrete examples to illustrate the data stories.