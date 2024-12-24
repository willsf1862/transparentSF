import os
import pandas as pd
import plotly.express as px
import datetime
import uuid
import logging
from tools.genAggregate import aggregate_data
from tools.anomaly_detection import filter_data_by_date_and_conditions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def generate_time_series_chart(
    context_variables: dict,
    time_series_field: str,
    numeric_fields,
    aggregation_period: str = 'day',
    group_field: str = None,
    agg_functions: dict = None,
    max_legend_items: int = 10,
    filter_conditions: dict = None,
    null_group_label: str = 'NA',
    show_average_line: bool = False
) -> str:
    try:
        logging.info("Full context_variables: %s", context_variables)

        if isinstance(numeric_fields, str):
            numeric_fields = [numeric_fields]
        logging.debug("Numeric fields: %s", numeric_fields)

        logging.debug("Context Variables: %s", context_variables)

        # Retrieve title and y_axis_label from context_variables
        chart_title = context_variables.get("chart_title", "Time Series Chart")
        y_axis_label = context_variables.get("y_axis_label", numeric_fields[0].capitalize())
        noun = context_variables.get("noun",y_axis_label)
        # Create a copy of the dataset to avoid modifying the original data
        original_df = context_variables.get("dataset")
        if original_df is None or original_df.empty:
            logging.error("Dataset is not available or is empty.")
            return "**Error**: Dataset is missing or empty. Please provide a valid dataset in 'context_variables'."

        df = original_df.copy()
        logging.debug("DataFrame copied to avoid in-place modifications.")

        # Store original column names for later reference
        original_columns = df.columns.tolist()
        column_mapping = {col.lower(): col for col in original_columns}
        logging.debug("Original column mapping: %s", column_mapping)
        
        # Convert columns to lowercase for case-insensitive comparison
        df.columns = df.columns.str.lower()
        logging.debug("Standardized DataFrame columns: %s", df.columns.tolist())
        
        # Convert input fields to lowercase for comparison
        time_series_field = time_series_field.lower()
        numeric_fields = [field.lower() for field in numeric_fields]
        if group_field:
            group_field = group_field.lower()
            logging.debug(f"Lowercased group_field: {group_field}")

        # Check for fields using case-insensitive comparison
        required_fields = [time_series_field] + numeric_fields
        if group_field:
            required_fields.append(group_field)
        missing_fields = [field for field in required_fields if field not in df.columns]

        if missing_fields:
            logging.error("Missing fields in DataFrame: %s. Available columns: %s", missing_fields, df.columns.tolist())
            return f"**Error**: Missing required fields in DataFrame: {missing_fields}. Please check the column names and ensure they are present."

        # Apply filter conditions if provided
        if filter_conditions:
            data_records = df.to_dict('records')
            # Apply filter_data_by_date_and_conditions
            filtered_data = filter_data_by_date_and_conditions(
                data_records,
                filter_conditions,
                start_date=None,
                end_date=None,
                date_field=time_series_field
            )
            # Convert back to DataFrame
            df = pd.DataFrame(filtered_data)
            logging.info(f"Filtered data size: {len(df)} records after applying filters.")
            if df.empty:
                logging.error("No data available after applying filters.")
                return "**Error**: No data available after applying filters. Please adjust your filter conditions."

        for field in numeric_fields:
            pre_conversion_count = df[field].notna().sum()
            df[field] = df[field].astype(str).str.strip()
            df[field] = pd.to_numeric(df[field], errors='coerce')
            post_conversion_count = df[field].notna().sum()
            coerced_count = pre_conversion_count - post_conversion_count

            if coerced_count > 0:
                logging.info("Field '%s': %d values were coerced to NaN during conversion to numeric.", field, coerced_count)
                if 'id' in df.columns:
                    coerced_values = df[df[field].isna()][['id', field]].head(20)
                    logging.info("Sample of coerced values in field '%s' (ID and Value):\n%s", field, coerced_values)
                else:
                    coerced_values = df[df[field].isna()].head(20)
                    logging.info("Sample of coerced values in field '%s' (Full Row):\n%s", field, coerced_values)

        # Drop rows with NaN in numeric fields
        df.dropna(subset=numeric_fields, inplace=True)
        logging.info("Dropped NA values from DataFrame.")
        
        if group_field and null_group_label:
            df[group_field] = df[group_field].fillna(null_group_label)
            df[group_field] = df[group_field].replace('', null_group_label)
        
        if agg_functions is None:
            agg_functions = {field: 'sum' for field in numeric_fields}
        logging.info("Aggregation functions: %s", agg_functions)

        aggregated_df = aggregate_data(
            df=df,
            time_series_field=time_series_field,
            numeric_fields=numeric_fields,
            aggregation_period=aggregation_period,
            group_field=group_field,
            agg_functions=agg_functions
        )

        if 'time_period' not in aggregated_df.columns:
            logging.error("'time_period' column is missing after aggregation.")
            return "**Error**: The 'time_period' column is missing after aggregation. Check the 'aggregate_data' function for proper time grouping."

        # Ensure 'time_period' is datetime and sort the DataFrame
        aggregated_df['time_period'] = pd.to_datetime(aggregated_df['time_period'])
        aggregated_df = aggregated_df.sort_values('time_period')
        logging.debug("Aggregated DataFrame sorted by 'time_period'.")

        # Compute values for the caption
        try:
            last_time_period = aggregated_df['time_period'].max()
            earliest_time_period = aggregated_df['time_period'].min()
            last_month_name = last_time_period.strftime('%B')
            logging.debug(f"Last time period: {last_time_period}, Month name: {last_month_name}")

            # Caption for all charts
            total_latest_month = aggregated_df[aggregated_df['time_period'] == last_time_period][numeric_fields[0]].sum()
            logging.debug(f"Total value for last period: {total_latest_month}")

            # Exclude the last period for calculating the average of the rest across all groups
            rest_periods = aggregated_df[aggregated_df['time_period'] < last_time_period]
            if rest_periods.empty:
                average_of_rest = 0
            else:
                # Sum over groups to get total per time period
                total_per_time_period = rest_periods.groupby('time_period')[numeric_fields[0]].sum()
                average_of_rest = total_per_time_period.mean()
                total_months = len(rest_periods['time_period'].unique())
            logging.debug(f"Average value of rest periods: {average_of_rest}")

            percentage_diff_total = ((total_latest_month - average_of_rest) / average_of_rest) * 100 if average_of_rest != 0 else 0
            above_below_total = 'above' if total_latest_month > average_of_rest else 'below'
            percentage_diff_total = abs(round(percentage_diff_total, 2))
            y_axis_label_lower = y_axis_label.lower()

            caption_total = f"In {last_month_name}, there were {total_latest_month} {noun}, which is {percentage_diff_total}% {above_below_total} the {total_months} month average of {average_of_rest:.2f}."
            logging.info(f"Caption for total: {caption_total}")

            # Caption for charts with a group_field
            caption_group = ""
            if group_field:
                last_period_df = aggregated_df[aggregated_df['time_period'] == last_time_period]
                numeric_values = last_period_df.groupby(group_field)[numeric_fields[0]].sum().to_dict()
                logging.debug(f"Numeric values for last period by group: {numeric_values}")

                # Calculate the average of the prior periods for each group
                prior_periods = aggregated_df[aggregated_df['time_period'] < last_time_period]
                average_of_prior = prior_periods.groupby(group_field)[numeric_fields[0]].mean().to_dict()
                logging.debug(f"Average values of prior periods by group: {average_of_prior}")

                captions_group = []
                for grp, value in numeric_values.items():
                    if grp not in average_of_prior or average_of_prior[grp] == 0:
                        # If there's no prior data for this group or average is zero, skip calculation.
                        continue
                    percentage_diff_group = ((value - average_of_prior[grp]) / average_of_prior[grp]) * 100
                    above_below_group = 'above' if value > average_of_prior[grp] else 'below'
                    percentage_diff_group = abs(round(percentage_diff_group, 2))
                captions_group.append(f"For {grp}, in {last_month_name}, there were {value} {noun}, which is {percentage_diff_group}% {above_below_group} the {total_months} month average of {average_of_prior[grp]:.2f}.")
                caption_group = " ".join(captions_group)

            if group_field:
                caption = f"{caption_total}\n\n{caption_group}"
            else:
                caption = caption_total

            # ---------------------------------------------------------------------
            #  NEW LOGIC FOR MORE THAN 2 YEARS OF DATA
            # ---------------------------------------------------------------------
            time_span_years = (last_time_period - earliest_time_period).days / 365
            if time_span_years > 2:
                try:
                    last_year_num = last_time_period.year
                    last_month_num = last_time_period.month
                    prior_year_num = last_year_num - 1

                    # Filter for current year, through last month in data
                    mask_current_year = (
                        aggregated_df['time_period'].dt.year == last_year_num
                    ) & (
                        aggregated_df['time_period'].dt.month <= last_month_num
                    )
                    current_year_df = aggregated_df[mask_current_year]
                    current_year_sum = current_year_df[numeric_fields[0]].sum()

                    # Filter for prior year, through the same last month
                    mask_prior_year = (
                        aggregated_df['time_period'].dt.year == prior_year_num
                    ) & (
                        aggregated_df['time_period'].dt.month <= last_month_num
                    )
                    prior_year_df = aggregated_df[mask_prior_year]
                    prior_year_sum = prior_year_df[numeric_fields[0]].sum()

                    if prior_year_sum != 0:
                        ytd_diff_pct = ((current_year_sum - prior_year_sum) / prior_year_sum) * 100
                        ytd_diff_pct = round(ytd_diff_pct, 2)
                        above_below_ytd = 'above' if current_year_sum > prior_year_sum else 'below'

                        ytd_caption = (
                            f"As of the end of {last_month_name}, YTD {last_year_num} is {current_year_sum} {noun}, "
                            f"which is {abs(ytd_diff_pct)}% {above_below_ytd} the YTD {prior_year_num} total of {prior_year_sum}."
                        )
                    else:
                        ytd_caption = (
                            f"As of the end of {last_month_name}, YTD {last_year_num} is {current_year_sum} {noun}. "
                            f"No comparable YTD data for {prior_year_num}."
                        )

                    caption = f"{caption}\n\n{ytd_caption}"

                except Exception as ytd_err:
                    logging.warning(f"Failed to compute YTD comparison: {ytd_err}")

        except Exception as e:
            logging.error("Failed to compute caption values: %s", e)
            caption = ""

        # Limit legend to top max_legend_items
        if group_field:
            group_totals = aggregated_df.groupby(group_field)[numeric_fields].sum().sum(axis=1)
            top_groups = group_totals.sort_values(ascending=False).head(max_legend_items).index.tolist()
            logging.info("Top groups based on total values: %s", top_groups)

            filtered_agg = aggregated_df[aggregated_df[group_field].isin(top_groups)]
            other_groups = set(aggregated_df[group_field].unique()) - set(top_groups)
            if other_groups:
                others_df = aggregated_df[aggregated_df[group_field].isin(other_groups)].copy()
                if not others_df.empty:
                    others_df[group_field] = 'Others'
                    filtered_agg = pd.concat([filtered_agg, others_df], ignore_index=True)
            aggregated_df = filtered_agg

        script_dir = os.path.dirname(os.path.abspath(__file__))
        static_dir = os.path.join(script_dir, '..', 'static')
        os.makedirs(static_dir, exist_ok=True)

        # Use a short unique ID for the filename
        chart_id = uuid.uuid4().hex[:6]
        image_filename = f"chart_{chart_id}.png"
        image_path = os.path.join(static_dir, image_filename)
        logging.debug("Image will be saved to: %s", image_path)

        try:
            if group_field:
                group_field_original = column_mapping.get(group_field, group_field)
                logging.debug(f"Original group field name from mapping: {group_field_original}")
                fig = px.area(
                    aggregated_df,
                    x='time_period',
                    y=numeric_fields[0],
                    color=group_field,
                    labels={
                        'time_period': time_series_field.capitalize(),
                        'value': y_axis_label,
                        group_field: group_field_original.capitalize()
                    }
                )
            else:
                # Use a line chart when there is no group field
                fig = px.line(
                    aggregated_df,
                    x='time_period',
                    y=numeric_fields[0],
                    labels={
                        'time_period': time_series_field.capitalize(),
                        'value': y_axis_label
                    }
                )

                # Add data labels with formatting if no group field
                fig.update_traces(
                    mode="lines+markers+text",
                    text=aggregated_df[numeric_fields[0]].apply(
                        lambda x: f"{x}"
                                  if x < 1_000 else (
                                      f"{int(x / 1_000)}K"
                                      if x < 999_950 else (
                                          f"{int(x / 1_000_000)}M"
                                          if x < 999_950_000 else f"{int(x / 1_000_000_000)}B"
                                      )
                                  )
                    ),
                    textposition="top center",
                    textfont=dict(size=10)  # Smaller font for labels
                )

            # Ensure Y-axis always starts at 0
            fig.update_yaxes(range=[0, None], fixedrange=False)  # Set fixedrange to False for zooming

            # Add average line if show_average_line is True
            if show_average_line:
                average_line = pd.Series(aggregated_df[numeric_fields[0]].mean(), index=aggregated_df['time_period'])
                fig.add_scatter(
                    x=average_line.index, 
                    y=average_line.values, 
                    mode='lines', 
                    name='Average', 
                    line=dict(width=2, color='blue')
                )

            fig.update_layout(
                legend=dict(
                    orientation="h",    
                    yanchor="bottom",
                    y=-0.40,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=8)  # Smaller font for legend
                ),
                legend_title_text=column_mapping.get(group_field, '').capitalize() if group_field else '',
                title={
                    'text': f"{chart_title} <br> by {column_mapping.get(group_field, group_field).capitalize()}" if group_field else chart_title,
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=16, family='Arial', color='black', weight='bold')
                },
                xaxis=dict(
                    title=dict(font=dict(size=14, family='Arial', color='black')),
                    tickfont=dict(size=10, family='Arial', color='black')
                ),
                yaxis=dict(
                    title=dict(font=dict(size=14, family='Arial', color='black')),
                    tickfont=dict(size=10, family='Arial', color='black')
                ),
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(family="Arial", size=10, color="black"),
                autosize=True,
                height=600,
                margin=dict(l=50, r=50, t=80, b=100)  # Increased margin for whitespace
            )

            # Highlight the last data point (only if no group_field)
            if not group_field and not aggregated_df.empty:
                last_point = aggregated_df.iloc[-1]
                last_x = last_point['time_period']
                last_y = last_point[numeric_fields[0]]
        
                fig.add_scatter(
                    x=[last_x],
                    y=[last_y],
                    mode='markers',
                    name='Current Month',
                    marker=dict(
                        size=12,
                        color='gold',
                        symbol='circle-open',
                        line=dict(width=2, color='gold')
                    ),
                    showlegend=False,
                    hoverinfo='skip'
                )

                fig.add_annotation(
                    x=last_x,
                    y=last_y,
                    text=f"{last_month_name}<br>{last_y} {y_axis_label}",
                    showarrow=True,
                    arrowhead=2,
                    font=dict(size=12, family='Arial', color='#333'),
                    arrowcolor='gold',
                    arrowwidth=1,
                    bgcolor='rgba(255, 255, 0, 0.7)',
                    bordercolor='gold',
                    borderwidth=1,
                    ax=-60,
                    ay=-20,
                )

            # Ensure Y-axis always starts at 0 after layout updates
            fig.update_layout(
                legend=dict(
                    orientation="h",    
                    yanchor="bottom",
                    y=-0.40,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=8)
                ),
                legend_title_text=column_mapping.get(group_field, '').capitalize() if group_field else '',
                title={
                    'text': f"{chart_title} by {column_mapping.get(group_field, group_field).capitalize()}" if group_field else chart_title,
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=16, family='Arial', color='black', weight='bold')
                },
                xaxis=dict(
                    title=dict(font=dict(size=12, family='Arial', color='black')),
                    tickfont=dict(size=8, family='Arial', color='black')
                ),
                yaxis=dict(
                    title=dict(font=dict(size=12, family='Arial', color='black')),
                    tickfont=dict(size=8, family='Arial', color='black')
                ),
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(family="Arial", size=10, color="black"),
            )

            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=True, gridcolor='lightgrey')

            # Add filter conditions annotation
            if filter_conditions:
                filter_conditions_str = ', '.join([f"{cond['field']} {cond['operator']} {cond['value']}" for cond in filter_conditions])
            else:
                filter_conditions_str = "No filter conditions provided"
            fig.add_annotation(
                text=f"Filter Conditions: {filter_conditions_str}",
                xref="paper", yref="paper",
                x=0.0, y=-0.5,
                showarrow=False,
                font=dict(size=10, family='Arial', color='black'),
                xanchor='left'
            )

            # Save the chart as an image
            fig.write_image(image_path, engine="kaleido")
            logging.info("Chart saved successfully at %s", image_path)
            relative_path = os.path.relpath(image_path, start=script_dir)
            
            # Prepare crosstabbed data
            if group_field:
                crosstab_df = aggregated_df.pivot_table(
                    index=group_field,
                    columns='time_period',
                    values=numeric_fields[0],
                    aggfunc='sum',
                    fill_value=0
                )
                # Format the date columns for better readability
                crosstab_df.columns = [col.strftime('%Y-%m-%d') for col in crosstab_df.columns]
            else:
                crosstab_df = aggregated_df.set_index('time_period')[[numeric_fields[0]]].T
                crosstab_df.columns = [col.strftime('%Y-%m-%d') for col in crosstab_df.columns]
                crosstab_df.index = [y_axis_label]

            # Convert crosstab to HTML table
            html_table = crosstab_df.to_html(classes='data-table', index=True)

            # Create markdown content
            markdown_content = f""" 
{context_variables.get("chart_title", "Time Series Chart")}
![Chart]({relative_path.replace(os.sep, '/')})
Caption: {caption}

```json
{aggregated_df.to_json(orient='records', date_format='iso')[:1000]}
```"""

            # Include crosstab data table with toggle in HTML content
            html_content = f'''
<div style="width:100%" id="chart_{chart_id}">
    {fig.to_html(full_html=False)}
    <div> 
        {caption}
    </div>
    <p>
        <a href="javascript:void(0);" onclick="toggleDataTable('data_table_{chart_id}')">Show Data</a>
    </p>
    <div id="data_table_{chart_id}" style="display:none;">
        {html_table}
    </div>
    
</div>

<script>
function toggleDataTable(tableId) {{
    var table = document.getElementById(tableId);
    if (table.style.display === "none") {{
        table.style.display = "block";
    }} else {{
        table.style.display = "none";
    }}
}}
</script>

<style>
.data-table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
}}
.data-table th, .data-table td {{
    border: 1px solid #dddddd;
    text-align: left;
    padding: 8px;
}}
.data-table tr:nth-child(even) {{
    background-color: #f9f9f9;
}}
</style>
'''

            logging.debug("Markdown content created with chart ID: %s", chart_id)

            return markdown_content, html_content


        except Exception as e:
            logging.error("Failed to generate or save chart: %s", e)
            return f"**Error**: An unexpected error occurred while generating the chart: {e}"

    except ValueError as ve:
        logging.error("ValueError in generate_time_series_chart: %s", ve)
        return f"**Error**: {ve}"

    except Exception as e:
        logging.error("Unexpected error in generate_time_series_chart: %s", e)
        return f"**Error**: An unexpected error occurred: {e}"
