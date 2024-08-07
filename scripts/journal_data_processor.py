# scripts/journal_data_processor.py

import pandas as pd
from pathlib import Path


def process_csv(input_file_path, output_file_path):
    # Read the CSV file
    df = pd.read_csv(input_file_path)

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
                "Value": total_days,
                "OA Start": oa_start,
                "OA End": oa_end,
            }
        )

    # Group by Activity ID and apply the calculation
    result = df.groupby("Activity ID").apply(calculate_value).reset_index()

    # Reorder columns
    result = result[["Geounit", "Activity ID", "Value", "OA Start", "OA End"]]

    # Save the result to a CSV file
    result.to_csv(output_file_path, index=False)

    return result


def main():
    # File paths
    project_root = Path(__file__).resolve().parents[1]
    input_file_path = project_root / "raw_data" / "global_journal_operatingtime.csv"
    output_folder = project_root / "processed_data"
    output_file_name = "processed_journal_operatingtime.csv"
    output_file_path = output_folder / output_file_name

    # Ensure output directory exists
    output_folder.mkdir(parents=True, exist_ok=True)

    # Process the CSV and create the output file
    output = process_csv(input_file_path, output_file_path)

    # Print summary of the output
    print(f"\nProcessing complete. Output saved to: {output_file_path}")
    print("\nOutput Summary:")
    print(output.describe())
    print("\nFirst few rows of the output:")
    print(output.head())
    print(f"\nTotal number of Activity IDs processed: {len(output)}")


if __name__ == "__main__":
    main()
