# scripts/process_tickets_data.py

import pandas as pd
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))


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


def merge_and_distribute_operating_days(tickets_df, journal_df):
    """
    Merge tickets data with journal data and distribute Operating Days.

    Args:
    tickets_df (pd.DataFrame): Preprocessed tickets dataframe
    journal_df (pd.DataFrame): Processed journal dataframe

    Returns:
    pd.DataFrame: Merged dataframe with distributed Operating Days
    """
    # Merge tickets data with journal data
    merged_df = pd.merge(
        tickets_df, journal_df[["Activity ID", "Value"]], on="Activity ID", how="left"
    )

    # Rename 'Value' to 'Operating Days'
    merged_df = merged_df.rename(columns={"Value": "Operating Days"})

    # Group by Activity ID and calculate the count of tickets per Activity ID
    activity_counts = (
        merged_df.groupby("Activity ID").size().reset_index(name="Ticket_Count")
    )

    # Merge the counts back to the main dataframe
    merged_df = pd.merge(merged_df, activity_counts, on="Activity ID", how="left")

    # Distribute Operating Days equally among tickets with the same Activity ID
    merged_df["Operating Days"] = (
        merged_df["Operating Days"] / merged_df["Ticket_Count"]
    )

    # Drop the temporary Ticket_Count column
    merged_df = merged_df.drop(columns=["Ticket_Count"])

    return merged_df


def process_tickets_data():
    """
    Process tickets data, merge with journal data, and save the result.
    """
    # Define file paths
    raw_tickets_path = project_root / "raw_data" / "global_tickets_wles_ops_data.csv"
    processed_journal_path = (
        project_root / "processed_data" / "processed_journal_operatingtime.csv"
    )
    output_path = (
        project_root / "processed_data" / "processed_tickets_wles_ops_data.csv"
    )

    # Load raw tickets data
    tickets_df = pd.read_csv(raw_tickets_path)

    # Preprocess tickets data
    processed_tickets_df = preprocess_tickets_data(tickets_df)

    # Load processed journal data
    journal_df = pd.read_csv(processed_journal_path)

    # Merge and distribute Operating Days
    final_df = merge_and_distribute_operating_days(processed_tickets_df, journal_df)

    # Save the processed data
    final_df.to_csv(output_path, index=False)
    print(f"Processed tickets data saved to: {output_path}")

    # Print summary statistics
    print("\nSummary Statistics:")
    print(final_df.describe())
    print("\nFirst few rows:")
    print(final_df.head())


if __name__ == "__main__":
    process_tickets_data()
