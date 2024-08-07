import os
import argparse
import pandas as pd


def create_codebase_file(
    directories,
    output_file,
    extensions=None,
    ignore_dirs=None,
    ignore_files=None,
    exclude_data=False,
):
    if extensions is None:
        extensions = [
            ".py",
            ".ipynb",
            ".sql",
            ".md",
            ".csv",
            ".xlsx",
        ]  # Including data file types
    if ignore_dirs is None:
        ignore_dirs = [".git", ".venv", "__pycache__"]
    if ignore_files is None:
        ignore_files = [".gitignore", ".DS_Store"]

    if exclude_data:
        extensions = [ext for ext in extensions if ext not in [".csv", ".xlsx"]]
        ignore_dirs.extend(["raw_data", "processed_data"])

    with open(output_file, "w", encoding="utf-8") as outfile:
        for directory in directories:
            for dirpath, dirnames, filenames in os.walk(directory):
                # Skip ignored directories
                dirnames[:] = [d for d in dirnames if d not in ignore_dirs]

                for filename in filenames:
                    if filename in ignore_files:
                        continue

                    if not any(filename.endswith(ext) for ext in extensions):
                        continue

                    filepath = os.path.join(dirpath, filename)
                    relative_path = os.path.relpath(filepath, directory)

                    outfile.write(f"\n\n--- File: {relative_path} ---\n\n")

                    if filename.endswith((".csv", ".xlsx")):
                        outfile.write(summarize_data_file(filepath))
                    else:
                        try:
                            with open(filepath, "r", encoding="utf-8") as infile:
                                outfile.write(infile.read())
                        except UnicodeDecodeError:
                            outfile.write(f"[Binary file: {relative_path}]\n")


def summarize_data_file(filepath):
    try:
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filepath.endswith(".xlsx"):
            df = pd.read_excel(filepath)

        summary = f"Shape: {df.shape}\n\n"
        summary += "Columns:\n" + "\n".join(df.columns) + "\n\n"
        summary += "Data Types:\n" + df.dtypes.to_string() + "\n\n"
        summary += "First 5 rows:\n" + df.head().to_string() + "\n\n"
        summary += "Description:\n" + df.describe().to_string() + "\n"

        return summary
    except Exception as e:
        return f"Error summarizing file: {str(e)}\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a summary file of the codebase including data files."
    )
    parser.add_argument(
        "--dirs",
        nargs="+",
        default=["."],
        help="Directories to process (default: current directory)",
    )
    parser.add_argument(
        "--output", default="project_codebase_summary.txt", help="Output file path"
    )
    parser.add_argument(
        "--exclude-data",
        action="store_true",
        help="Exclude data files (.csv, .xlsx) from the summary",
    )
    args = parser.parse_args()

    create_codebase_file(args.dirs, args.output, exclude_data=args.exclude_data)
    print(f"Codebase summary created at: {args.output}")
