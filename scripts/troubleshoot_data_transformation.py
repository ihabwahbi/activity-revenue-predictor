import pandas as pd
from pathlib import Path


def clean_data(df):
    """
    Clean the dataframe by replacing NaN values with appropriate placeholders.
    """
    df = df.copy()

    categorical_columns = [
        "Sl Geounit (Code)",
        "Country Name",
        "Job Group code",
        "Job Type code",
        "Activity ID",
        "Booking Status",
        "Field Ticket ID",
        "Well Name",
        "Rig Name",
        "Rig type",
        "Well type",
        "Well Operating Environment",
        "Billing Account",
        "Field Ticket Status",
        "Rig environment",
        "Well Geometry",
    ]

    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    numerical_columns = df.select_dtypes(include=["int64", "float64"]).columns
    for col in numerical_columns:
        df[col] = df[col].fillna(0)

    return df


def adjust_month(date):
    """
    Adjust the month based on the specified criteria.
    Dates from the 26th onwards fall under the subsequent month.
    """
    if date.day <= 25:
        return date.replace(day=1)
    else:
        next_month = date + pd.DateOffset(months=1)
        return next_month.replace(day=1)


def preprocess_tickets_data(df):
    """
    Preprocess the tickets data.
    """
    df = df.copy()
    df["Field Ticket Start Date"] = pd.to_datetime(df["Field Ticket Start Date"])
    df["Field Ticket End Date"] = pd.to_datetime(df["Field Ticket End Date"])
    df["Adjusted Date"] = df["Field Ticket End Date"].apply(adjust_month)
    return df


def merge_and_distribute_operating_days(tickets_df, journal_df):
    """
    Merge tickets data with journal data and distribute Operating Days.
    Also calculate Operating_CellMonth based on the number of days in each month.
    """
    merged_df = pd.merge(
        tickets_df, journal_df[["Activity ID", "Value"]], on="Activity ID", how="left"
    )
    merged_df = merged_df.rename(columns={"Value": "Operating Days"})

    activity_counts = (
        merged_df.groupby("Activity ID").size().reset_index(name="Ticket_Count")
    )
    merged_df = pd.merge(merged_df, activity_counts, on="Activity ID", how="left")
    merged_df["Operating Days"] = (
        merged_df["Operating Days"] / merged_df["Ticket_Count"]
    )
    merged_df["Days_in_Month"] = merged_df["Adjusted Date"].dt.daysinmonth
    merged_df["Operating_CellMonth"] = (
        merged_df["Operating Days"] / merged_df["Days_in_Month"]
    )
    merged_df = merged_df.drop(columns=["Ticket_Count", "Days_in_Month"])

    return merged_df


def group_and_aggregate_tickets_data(df):
    """
    Group and aggregate the tickets data based on specified columns.
    """
    print(
        f"Total revenue before grouping: ${df['Field Ticket USD net value'].sum():,.2f}"
    )

    grouping_columns = [
        "Adjusted Date",
        "Sl Geounit (Code)",
        "Country Name",
        "Job Group code",
        "Job Type code",
        "Billing Account",
        "Rig Name",
        "Rig type",
        "Rig environment",
        "Well type",
        "Well Operating Environment",
    ]

    grouped_df = (
        df.groupby(grouping_columns)
        .agg(
            {
                "Well Name": "nunique",
                "Field Ticket USD net value": "sum",
                "Operating Days": "sum",
                "Operating_CellMonth": "sum",
            }
        )
        .reset_index()
    )

    print(
        f"Total revenue after grouping: ${grouped_df['Field Ticket USD net value'].sum():,.2f}"
    )

    zero_revenue_rows = grouped_df[grouped_df["Field Ticket USD net value"] == 0]
    if not zero_revenue_rows.empty:
        print(f"Warning: {len(zero_revenue_rows)} grouped rows have zero revenue")
        print("Sample of zero revenue rows:")
        print(zero_revenue_rows.head())

    grouped_df = grouped_df.rename(
        columns={
            "Well Name": "Unique_Well_Count",
            "Field Ticket USD net value": "Tickets_Revenue",
        }
    )

    return grouped_df


def main():
    # Define file paths
    project_root = Path(__file__).resolve().parents[1]
    raw_tickets_path = project_root / "raw_data" / "global_tickets_wles_ops_data.csv"
    processed_journal_path = (
        project_root / "processed_data" / "processed_journal_operatingtime.csv"
    )
    output_path = (
        project_root / "processed_data" / "processed_tickets_wles_ops_data.csv"
    )

    # Load and clean raw tickets data
    print("Loading and cleaning raw tickets data...")
    tickets_df = pd.read_csv(raw_tickets_path)
    tickets_df = clean_data(tickets_df)

    print("NaN values after cleaning:")
    print(tickets_df.isna().sum())

    # Preprocess tickets data
    print("\nPreprocessing tickets data...")
    processed_tickets_df = preprocess_tickets_data(tickets_df)

    # Load processed journal data
    print("Loading processed journal data...")
    journal_df = pd.read_csv(processed_journal_path)

    # Merge and distribute Operating Days
    print("Merging tickets data with journal data and distributing Operating Days...")
    merged_df = merge_and_distribute_operating_days(processed_tickets_df, journal_df)

    # Group and aggregate the data
    print("\nGrouping and aggregating the data...")
    final_df = group_and_aggregate_tickets_data(merged_df)

    # Save the processed data
    final_df.to_csv(output_path, index=False)
    print(f"\nProcessed tickets data saved to: {output_path}")

    # Print summary statistics
    print("\nSummary Statistics:")
    print(final_df.describe())
    print("\nFirst few rows of the final dataframe:")
    print(final_df.head())

    # Additional analysis for APG Geounit in November 2022
    apg_nov_2022 = final_df[
        (final_df["Sl Geounit (Code)"] == "APG")
        & (pd.to_datetime(final_df["Adjusted Date"]).dt.year == 2022)
        & (pd.to_datetime(final_df["Adjusted Date"]).dt.month == 11)
    ]
    print("\nTotal revenue for APG Geounit in November 2022:")
    print(f"${apg_nov_2022['Tickets_Revenue'].sum():,.2f}")


if __name__ == "__main__":
    main()
