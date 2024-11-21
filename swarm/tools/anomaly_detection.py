import datetime
from datetime import date
import logging
import pandas as pd
from tools.generateAnomalyCharts import generate_anomalies_summary_with_charts 
from dateutil import parser  # Import this library for robust date parsing

# set logging level to INFO
logging.basicConfig(level=logging.INFO)
def find_key(item, field_name):
    if field_name in item:
        return field_name
    for key in item.keys():
        if key.lower() == field_name.lower():
            return key
    return None

def get_item_value_case_insensitive(item, field_name):
    for key in item.keys():
        if key.lower() == field_name.lower():
            return item[key]
    return None

def calculate_stats(values):
    n = len(values)
    if n == 0:
        return {'mean': None, 'stdDev': None}
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std_dev = variance ** 0.5
    return {'mean': int(mean), 'stdDev': int(std_dev)}

def get_date_ranges():
    from datetime import date, timedelta

    today = date.today()

    # Calculate recent period: last full month (previous month)
    first_day_of_current_month = date(today.year, today.month, 1)
    recent_end = first_day_of_current_month - timedelta(days=1)  # Last day of previous month
    recent_start = date(recent_end.year, recent_end.month, 1)    # First day of previous month

    # Calculate comparison period: 12 months before recentStart
    comparison_end = recent_start - timedelta(days=1)            # Day before recentStart
    comparison_start = date(recent_start.year - 1, recent_start.month, 1)  # Same month and day, previous year

    print("Recent period:", recent_start, "to", recent_end)
    print("Comparison period:", comparison_start, "to", comparison_end)
    
    return {
        'recentPeriod': {
            'start': recent_start,
            'end': recent_end,
        },
        'comparisonPeriod': {
            'start': comparison_start,
            'end': comparison_end,
        },
    }

def generate_anomalies_summary(results):
    anomalies = []
    for item in results:
        group = item.get('group_value', 'Unknown Group')
        recent_mean = item.get('recent_mean')
        comparison_mean = item.get('comparison_mean')
        difference = item.get('difference')
        std_dev = item.get('stdDev', 0)

        # Determine the direction of the difference
        if difference > 0:
            direction = "higher"
        elif difference < 0:
            direction = "lower"
        else:
            direction = "the same as"

        # Absolute value of difference for reporting
        abs_difference = abs(difference)

        # Format the anomaly message
        anomaly_message = (
            f"Anomaly detected in '{group}': "
            f"Recent mean (RM: {recent_mean:.2f}) is {direction} than the comparison mean (CM: {comparison_mean:.2f}) "
            f"by {abs_difference:.2f} units. "
            f"Standard Deviation: {std_dev:.2f}."
        )

        anomalies.append(anomaly_message)

    # Create the summary message
    summary = "\n".join(anomalies) if anomalies else "No significant anomalies were detected."

    # Log the summary
    logging.info(f"Anomalies summary:\n{summary}")
    # Convert the summary into light HTML
    html_summary = "<html><body><h2>Anomalies Summary</h2><ul>"
    for anomaly in anomalies:
        html_summary += f"<li>{anomaly}</li>"
    html_summary += "</ul></body></html>"

    # Log the HTML summary
    logging.info(f"Anomalies summary in HTML:\n{html_summary}")
    return {"anomalies": html_summary}

def group_data_by_field_and_date(data_array, group_field, numeric_field, date_field=None):
    logging.info(f"Grouping data by '{group_field}' field.")
    invalid_records = []

    for idx, item in enumerate(data_array):
        try:
            # Handle group_field
            key_group_field = find_key(item, group_field)
            if key_group_field is None:
                raise ValueError(f"Missing {group_field}.")
            group_value = item[key_group_field]
            if group_value is None:
                raise ValueError(f"Missing value for {group_field}.")
            item[group_field] = group_value  # Ensure the key is consistent

            # Handle numeric_field
            key_numeric_field = find_key(item, numeric_field)
            if key_numeric_field is None:
                raise ValueError(f"Missing {numeric_field}.")
            numeric_value = item[key_numeric_field]
            if numeric_value is None:
                raise ValueError(f"Missing value for {numeric_field}.")
            if isinstance(numeric_value, str):
                numeric_value = numeric_value.replace(',', '')
            numeric_value = float(numeric_value)
            item[numeric_field] = numeric_value

            # Handle date_field if provided
            if date_field:
                key_date_field = find_key(item, date_field)
                if key_date_field is None:
                    raise ValueError(f"Missing {date_field}.")
                date_value = item[key_date_field]
                if date_value is None:
                    raise ValueError(f"Missing value for {date_field}.")
                if isinstance(date_value, str):
                    date_obj = custom_parse_date(date_value)
                    if date_obj is None:
                        raise ValueError("Invalid date format")
                    item[date_field] = date_obj
                elif isinstance(date_value, (datetime.date, datetime.datetime)):
                    date_obj = date_value.date() if isinstance(date_value, datetime.datetime) else date_value
                    item[date_field] = date_obj
                else:
                    raise ValueError(f"Unrecognized {date_field} type: {type(date_value)}.")
        except Exception as e:
            logging.warning(f"Record {idx}: {e}. Skipping.")
            invalid_records.append(idx)
            continue

    # Remove invalid records in reverse order to maintain correct indexing
    for idx in sorted(invalid_records, reverse=True):
        del data_array[idx]
        logging.debug(f"Removed invalid record at index {idx}.")

    # Proceed only if there are valid records left
    if not data_array:
        logging.error("All records have been filtered out. Please check your data and parsing logic.")
        return {}

    # Now perform the grouping
    grouped = {}
    for item in data_array:
        group_value = item[group_field]

        # If date_field is provided, group by date as well
        if date_field:
            date_obj = item[date_field]
            if not date_obj:
                continue  # Shouldn't happen, but added for safety

            # Create date_key in 'YYYY-MM' format
            date_key = date_obj.strftime("%Y-%m")
        else:
            date_key = 'All'  # Use a constant key when date_field is not provided

        if group_value not in grouped:
            grouped[group_value] = {}
        if date_key not in grouped[group_value]:
            grouped[group_value][date_key] = 0

        # Get the numeric value from the specified numeric_field
        numeric_value = item[numeric_field]
        grouped[group_value][date_key] += numeric_value

    logging.info(f"Grouping completed. Total groups: {len(grouped)}")
    return grouped

def custom_parse_date(date_str):
    # Define potential date formats to try
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y", "%d/%m/%Y"]  # Added more formats
    for fmt in date_formats:
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    # Try using dateutil.parser as a fallback
    try:
        return parser.parse(date_str, dayfirst=True).date()
    except (ValueError, parser.ParserError):
        # print(f"WARNING: Invalid date format for record: {date_str}. Skipping.")
        return None  # Return None if the date is invalid

def filter_data_by_date_and_conditions(data, filter_conditions, start_date=None, end_date=None, date_field=None):
    # Initialize error log
    error_log = []

    # If start_date and end_date are provided, ensure they are datetime.date objects
    if start_date is not None:
        if isinstance(start_date, str):
            start_date = custom_parse_date(start_date)
        elif not isinstance(start_date, datetime.date):
            error_log.append("Invalid start_date format. Please provide a valid date.")
            start_date = None
    if end_date is not None:
        if isinstance(end_date, str):
            end_date = custom_parse_date(end_date)
        elif not isinstance(end_date, datetime.date):
            error_log.append("Invalid end_date format. Please provide a valid date.")
            end_date = None

    # Parse date values in filter_conditions
    for condition in filter_conditions:
        if isinstance(condition['value'], str):
            parsed_date = custom_parse_date(condition['value'])
            if parsed_date:
                condition['value'] = parsed_date
                condition['is_date'] = True  # Mark this condition as a date comparison
            else:
                condition['is_date'] = False
                error_log.append(f"Condition value for field '{condition['field']}' is not a date.")
        else:
            condition['is_date'] = isinstance(condition['value'], datetime.date)

    filtered_data = []
    for idx, item in enumerate(data):
        # Initialize meets_conditions to True
        meets_conditions = True

        # If date_field and date range are provided, perform date filtering
        if date_field and start_date and end_date:
            key_date_field = find_key(item, date_field)
            if key_date_field is None:
                error_log.append(f"Record {idx} missing {date_field} information. Skipping.")
                continue
            date_value = item[key_date_field]
            if date_value is None:
                error_log.append(f"Record {idx} missing {date_field} information. Skipping.")
                continue

            # Convert date_field value to datetime.date if necessary
            if isinstance(date_value, str):
                item_date = custom_parse_date(date_value)
            elif isinstance(date_value, pd.Timestamp):
                item_date = date_value.date()
            elif isinstance(date_value, (datetime.date, datetime.datetime)):
                item_date = date_value.date() if isinstance(date_value, datetime.datetime) else date_value
            elif isinstance(date_value, (int, float)):
                try:
                    date_str = str(int(date_value))
                    item_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                except (ValueError, TypeError) as e:
                    error_log.append(f"Record {idx}: Invalid numeric date for {date_field} {e}. Skipping.")
                    continue
            else:
                error_log.append(f"Record {idx}: Unrecognized {date_field} type: {type(date_value)}. Skipping.")
                continue

            if item_date is None:
                error_log.append(f"Record {idx}: Invalid date format for {date_field}. Skipping.")
                continue  # Skip records with invalid dates

            # Perform date filtering based on start_date and end_date
            if not (start_date <= item_date <= end_date):
                continue  # Skip this record

            # Update item with parsed date
            item[date_field] = item_date

        # Now, check the filter_conditions
        for condition in filter_conditions:
            field = condition['field']
            operator = condition['operator']
            value = condition['value']
            is_date = condition.get('is_date', False)

            key_field = find_key(item, field)
            if key_field is None:
                meets_conditions = False
                error_log.append(f"Record {idx}: Missing field '{field}' for condition '{field} {operator} {value}'. Skipping.")
                break
            item_value = item[key_field]

            # If item_value is None, we can't compare
            if item_value is None:
                meets_conditions = False
                error_log.append(f"Record {idx}: Missing value for field '{field}'. Skipping.")
                break

            # Parse item_value if condition value is a date
            if is_date:
                if isinstance(item_value, str):
                    item_value = custom_parse_date(item_value)
                elif isinstance(item_value, (datetime.date, datetime.datetime)):
                    item_value = item_value.date() if isinstance(item_value, datetime.datetime) else item_value
                elif isinstance(item_value, (int, float)):
                    try:
                        date_str = str(int(item_value))
                        item_value = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                    except (ValueError, TypeError):
                        meets_conditions = False
                        error_log.append(f"Record {idx}: Invalid numeric date for field '{field}'. Skipping.")
                        break
                else:
                    meets_conditions = False
                    error_log.append(f"Record {idx}: Unrecognized type for field '{field}': {type(item_value)}. Skipping.")
                    break

                if item_value is None:
                    meets_conditions = False
                    error_log.append(f"Record {idx}: Invalid date for field '{field}'. Skipping.")
                    break

                # Perform date comparison
                try:
                    if operator == '>' and not (item_value > value):
                        meets_conditions = False
                        break
                    elif operator == '<' and not (item_value < value):
                        meets_conditions = False
                        break
                    elif operator == '>=' and not (item_value >= value):
                        meets_conditions = False
                        break
                    elif operator == '<=' and not (item_value <= value):
                        meets_conditions = False
                        break
                    elif operator == '==' and not (item_value == value):
                        meets_conditions = False
                        break
                    elif operator == '!=' and not (item_value != value):
                        meets_conditions = False
                        break
                except Exception as e:
                    meets_conditions = False
                    error_log.append(f"Record {idx}: Error during date comparison for field '{field}': {e}. Skipping.")
                    break
            else:
                # Existing code for non-date fields...
                # Clean and convert values if numeric
                if isinstance(item_value, str):
                    item_value = item_value.replace(',', '')

                try:
                    item_value_float = float(item_value)
                    value_float = float(value)
                    comparison_possible = True
                except (ValueError, TypeError):
                    item_value_float = item_value
                    value_float = value
                    comparison_possible = False

                # Condition checks for non-date fields
                try:
                    if operator == '==' and item_value != value:
                        meets_conditions = False
                        break
                    elif operator == '!=' and item_value == value:
                        meets_conditions = False
                        break
                    elif operator in ['<', '<=', '>', '>=']:
                        if not comparison_possible:
                            meets_conditions = False
                            error_log.append(f"Record {idx}: Cannot compare non-numeric values for field '{field}' with operator '{operator}'. Skipping.")
                            break
                        if operator == '<' and not (item_value_float < value_float):
                            meets_conditions = False
                            break
                        elif operator == '<=' and not (item_value_float <= value_float):
                            meets_conditions = False
                            break
                        elif operator == '>' and not (item_value_float > value_float):
                            meets_conditions = False
                            break
                        elif operator == '>=' and not (item_value_float >= value_float):
                            meets_conditions = False
                            break
                except Exception as e:
                    meets_conditions = False
                    error_log.append(f"Record {idx}: Error during comparison for field '{field}': {e}. Skipping.")
                    break

        if meets_conditions:
            filtered_data.append(item)

    # At the end of the loop, log the error summary
    total_records = len(data)
    filtered_records = len(filtered_data)
    invalid_records = total_records - filtered_records
    logging.info(f"Total records processed: {total_records}")
    logging.info(f"Total records after filtering: {filtered_records}")
    logging.info(f"Total invalid records: {invalid_records}")
    if error_log:
        logging.info("Error details:")
        for error in error_log:
            logging.info(error)

    return filtered_data

def group_data_by_field_and_date(data_array, group_field, numeric_field, date_field=None):
    logging.info(f"Grouping data by '{group_field}' field.")
    invalid_records = []
    distinct_dates = set()
    for item in data_array:
        if date_field:
            date_value = item.get(date_field)
            if date_value:
                distinct_dates.add(date_value.strftime("%Y-%m"))
                
    logging.info(f"Distinct {date_field} values: {len(distinct_dates)}")
    for idx, item in enumerate(data_array):
        try:
            # Handle group_field
            group_value = item.get(group_field)
            if group_value is None:
                raise ValueError(f"Missing {group_field}.")

            # Handle numeric_field
            numeric_value = item.get(numeric_field)
            if numeric_value is None:
                raise ValueError(f"Missing {numeric_field}.")
            if isinstance(numeric_value, str):
                numeric_value = numeric_value.replace(',', '')
            numeric_value = float(numeric_value)
            item[numeric_field] = numeric_value

            # Handle date_field if provided
            if date_field:
                date_value = item.get(date_field)
                if date_value is None:
                    raise ValueError(f"Missing {date_field}.")
                if isinstance(date_value, str):
                    date_obj = custom_parse_date(date_value)
                    if date_obj is None:
                        raise ValueError("Invalid date format")
                    item[date_field] = date_obj
                elif isinstance(date_value, (datetime.date, datetime.datetime)):
                    date_obj = date_value.date() if isinstance(date_value, datetime.datetime) else date_value
                    item[date_field] = date_obj
                else:
                    raise ValueError(f"Unrecognized {date_field} type: {type(date_value)}.")
        except Exception as e:
            logging.warning(f"Record {idx}: {e}. Skipping.")
            invalid_records.append(idx)
            continue

    # Remove invalid records in reverse order to maintain correct indexing
    for idx in sorted(invalid_records, reverse=True):
        del data_array[idx]
        logging.debug(f"Removed invalid record at index {idx}.")

    # Proceed only if there are valid records left
    if not data_array:
        logging.error("All records have been filtered out. Please check your data and parsing logic.")
        return {}

    # Now perform the grouping
    grouped = {}
    for item in data_array:
        key = item.get(group_field, 'Unknown')

        # If date_field is provided, group by date as well
        if date_field:
            date_obj = item.get(date_field)
            if not date_obj:
                continue  # Shouldn't happen, but added for safety

            # Create date_key in 'YYYY-MM' format
            date_key = date_obj.strftime("%Y-%m")
        else:
            date_key = 'All'  # Use a constant key when date_field is not provided

        if key not in grouped:
            grouped[key] = {}
        if date_key not in grouped[key]:
            grouped[key][date_key] = 0

        # Get the numeric value from the specified numeric_field
        numeric_value = item.get(numeric_field, 0)
        grouped[key][date_key] += numeric_value

    logging.info(f"Grouping completed. Total groups: {len(grouped)}")
    return grouped

def get_month_range(start_date, end_date):
    from dateutil.relativedelta import relativedelta
    months = []
    current_date = start_date.replace(day=1)
    while current_date <= end_date:
        months.append(current_date.strftime("%Y-%m"))
        current_date += relativedelta(months=1)
    return months


def anomaly_detection(
    context_variables,
    group_field=None,
    filter_conditions=[],
    min_diff=2,
    recent_period=None,
    comparison_period=None,
    date_field=None,
    numeric_field=None,
    y_axis_label=None,
    title=None
):
    # (Logging and initial data retrieval remain the same)
    
    data = context_variables.get("dataset")
    if data is None:
        logging.error("Dataset is not available.")
        return {"error": "Dataset is not available."}

    data_records = data.to_dict('records')
    logging.info(f"Total records in the dataset: {len(data_records)}")

    # Adjust date periods if date_field is provided
    if date_field and (recent_period is None or comparison_period is None):
        date_ranges = get_date_ranges()
        if recent_period is None:
            recent_period = date_ranges['recentPeriod']
        if comparison_period is None:
            comparison_period = date_ranges['comparisonPeriod']

    # Convert recent_period and comparison_period to date objects if they are provided
    if recent_period:
        for key in ['start', 'end']:
            if isinstance(recent_period[key], str):
                recent_period[key] = datetime.datetime.strptime(recent_period[key], "%Y-%m-%d").date()
    if comparison_period:
        for key in ['start', 'end']:
            if isinstance(comparison_period[key], str):
                comparison_period[key] = datetime.datetime.strptime(comparison_period[key], "%Y-%m-%d").date()

    # Log date periods
    if date_field:
        logging.info(f"Recent period: {recent_period['start']} to {recent_period['end']}")
        logging.info(f"Comparison period: {comparison_period['start']} to {comparison_period['end']}")
    else:
        logging.info("Date field not provided. Skipping date-based filtering.")

    # Apply filtering
    recent_data = filter_data_by_date_and_conditions(
        data_records,
        filter_conditions,
        start_date=comparison_period['start'] if comparison_period else None,
        end_date=recent_period['end'] if recent_period else None,
        date_field=date_field
    )

    logging.info(f"Filtered data size: {len(recent_data)} records after applying filters.")

    # Group data
    grouped_data = group_data_by_field_and_date(
        recent_data,
        group_field,
        numeric_field,
        date_field=date_field
    )
    logging.info(f"Grouped data size: {len(grouped_data)} groups.")
    print(grouped_data)
    # Get the full list of months between comparison_period['start'] and recent_period['end']
    if date_field:
        full_months = get_month_range(comparison_period['start'], recent_period['end'])
    else:
        full_months = ['All']  # When date_field is not provided

    results = []
    for group_value, data_points in grouped_data.items():
        logging.info(f"Processing group: {group_value}")

        # Expand data_points to include zeros for missing months
        for month in full_months:
            if month not in data_points:
                data_points[month] = 0

        dates = sorted(data_points.keys())
        counts = [data_points[date] for date in dates]

        logging.debug(f"Dates in group {group_value}: {dates}")
        logging.debug(f"Counts in group {group_value}: {counts}")

        comparison_counts = []
        recent_counts = []

        for date_key in dates:
            count = data_points[date_key]

            if date_field:
                item_date = datetime.datetime.strptime(date_key, "%Y-%m").date()
                if comparison_period['start'] <= item_date <= comparison_period['end']:
                    comparison_counts.append(count)
                elif recent_period['start'] <= item_date <= recent_period['end']:
                    recent_counts.append(count)
            else:
                # If date_field is not provided, consider all counts for comparison
                comparison_counts = counts
                recent_counts = counts
                break  # No need to continue the loop

        logging.info(f"Comparison counts for {group_value}: {comparison_counts}")
        logging.info(f"Recent counts for {group_value}: {recent_counts}")

        # Initialize stats with default empty values
        comparison_stats = {'mean': None, 'stdDev': None}
        recent_stats = {'mean': None, 'stdDev': None}

        if comparison_counts:
            comparison_stats = calculate_stats(comparison_counts)
        if recent_counts:
            recent_stats = calculate_stats(recent_counts)

        logging.info(f"Comparison stats for {group_value}: {comparison_stats}")
        logging.info(f"Recent stats for {group_value}: {recent_stats}")

        if (
            comparison_stats['mean'] is not None and
            recent_stats['mean'] is not None and
            comparison_stats['stdDev'] is not None
        ):
            try:
                comparison_mean = float(comparison_stats['mean'])
                recent_mean = float(recent_stats['mean'])
                comparison_std_dev = float(comparison_stats['stdDev'])
                min_diff = float(min_diff)
            except ValueError as e:
                logging.error(f"Error converting statistics to float: {e}")
                continue

            difference = recent_mean - comparison_mean

            if comparison_std_dev > 0:
                out_of_bounds = (
                    abs(difference) > comparison_std_dev * min_diff and
                    comparison_mean > 2 and
                    recent_mean > 2
                )

                logging.debug(f"Difference for {group_value}: {difference}")
                logging.debug(f"Out of bounds for {group_value}: {out_of_bounds}")

                if out_of_bounds:
                    results.append({
                        'group_value': group_value,
                        'comparison_mean': comparison_mean,
                        'recent_mean': recent_mean,
                        'difference': difference,
                        'stdDev': comparison_std_dev,
                        'dates': dates,
                        'counts': counts,
                        'out_of_bounds': out_of_bounds
                    })

    results.sort(key=lambda x: abs(x['difference']), reverse=True)
    
    metadata = {
        'recent_period': recent_period,
        'comparison_period': comparison_period,
        'group_field': group_field,
        'date_field': date_field,
        'y_axis_label': y_axis_label,
        'title': title,
        'filter_conditions': filter_conditions
    }

    html_content = generate_anomalies_summary_with_charts(results, metadata)
    return  {"anomalies":html_content}
    # return generate_anomalies_summary(results)