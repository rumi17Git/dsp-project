import pandas as pd
import numpy as np
import os
import argparse

def split_dataset(input_path, output_folder, num_files):
    # Read the dataset
    df = pd.read_csv(input_path)

    # ✅ Ensure TotalCharges column exists and clean it
    if "TotalCharges" not in df.columns:
        raise ValueError("❌ The 'TotalCharges' column is missing from your dataset!")

    # Convert to numeric and replace invalid values (empty strings, spaces, etc.)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce").fillna(0)

    # Make sure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Split into equal parts
    chunks = np.array_split(df, num_files)

    # Save each chunk to raw-data
    for i, chunk in enumerate(chunks):
        file_path = os.path.join(output_folder, f"chunk_{i}.csv")
        chunk.to_csv(file_path, index=False)
        print(f"✅ Saved {file_path} with {len(chunk)} rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into multiple CSV files for ingestion.")
    parser.add_argument("--input", type=str, required=True, help="Path to the dataset CSV")
    parser.add_argument("--output", type=str, default="raw-data", help="Output folder (default: raw-data)")
    parser.add_argument("--num_files", type=int, required=True, help="Number of files to create")

    args = parser.parse_args()

    split_dataset(args.input, args.output, args.num_files)
