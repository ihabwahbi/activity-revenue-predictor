# utils/journal_data_preprocessing.py

import pandas as pd


def preprocess_journal_data(df):
    """
    Preprocess the journal operating time data.

    Args:
    df (pd.DataFrame): Raw journal operating time dataframe

    Returns:
    pd.DataFrame: Processed dataframe with calculated values for each unique Activity ID
    """
    # Convert date columns to datetime
    df["Journal Activity start time"] = pd.to_datetime(
        df["Journal Activity start time"]
    )
    df["Journal Activity end time"] = pd.to_datetime(df["Journal Activity end time"])

    # Sort the dataframe by Activity ID and start time
    df = df.sort_values(["Activity ID", "Journal Activity start time"])

    # Function to calculate the value for each Activity ID group
    def calculate_value(group):
        total_days = 0
        i = 0
        oa_start = group.iloc[0]["Journal Activity start time"]
        oa_end = group.iloc[-1]["Journal Activity end time"]
        geounit = group.iloc[0]["Sl Geounit (Code)"]

        while i < len(group):
            date1 = group.iloc[i]["Journal Activity start time"].date()
            date2 = None
            j = i

            while j < len(group) - 1:
                current_end = group.iloc[j]["Journal Activity end time"].date()
                next_start = group.iloc[j + 1]["Journal Activity start time"].date()

                if current_end != next_start:
                    date2 = current_end
                    break
                j += 1

            if date2 is None:
                date2 = group.iloc[-1]["Journal Activity end time"].date()

            days = (
                date2 - date1
            ).days + 1  # Adding 1 to include both start and end dates
            total_days += days

            if j == len(group) - 1:
                break

            i = j + 1  # Move to the next unprocessed row

        return pd.Series(
            {
                "Geounit": geounit,
                "OA Start": oa_start,
                "OA End": oa_end,
                "Value": total_days,
            }
        )

    # Group by Activity ID and apply the calculation
    result = df.groupby("Activity ID").apply(calculate_value).reset_index()

    # Reorder columns
    result = result[["Geounit", "Activity ID", "OA Start", "OA End", "Value"]]

    return result
