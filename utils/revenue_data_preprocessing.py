# utils/revenue_data_preprocessing.py

import pandas as pd


def preprocess_tickets_data(df):
    """
    Preprocess the tickets data.

    Args:
    df (pd.DataFrame): Raw tickets dataframe

    Returns:
    pd.DataFrame: Preprocessed tickets dataframe
    """
    df = df.copy()
    df["Field Ticket Start Date"] = pd.to_datetime(df["Field Ticket Start Date"])
    df["Field Ticket End Date"] = pd.to_datetime(df["Field Ticket End Date"])
    df["Adjusted Date"] = df["Field Ticket End Date"].apply(adjust_month)
    return df


def preprocess_rpe_data(df):
    """
    Preprocess the RPE revenue data.
    Filters the data to include only WLES (Wireline Services) business line
    and Service Revenue GL Account Category.

    Args:
    df (pd.DataFrame): Raw RPE revenue dataframe

    Returns:
    pd.DataFrame: Preprocessed RPE revenue dataframe for WLES and Service Revenue only
    """
    df = df.copy()
    df["Month Date"] = pd.to_datetime(df["Month Date"])

    # Filter for WLES business line and Service Revenue GL Account Category
    df_filtered = df[
        (df["SL Sub Business Line (Code)"] == "WLES")
        & (df["GL Account Category"] == "Service Revenue")
    ]

    return df_filtered


def adjust_month(date):
    """
    Adjust the month based on the specified criteria.
    Dates from the 26th onwards fall under the subsequent month.
    This function is used only for tickets data.

    Args:
    date (pd.Timestamp): Date to adjust

    Returns:
    pd.Timestamp: Adjusted date
    """
    if date.day <= 25:
        return date.replace(day=1)
    else:
        next_month = date + pd.DateOffset(months=1)
        return next_month.replace(day=1)
