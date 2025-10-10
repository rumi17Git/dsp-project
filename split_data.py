import pandas as pd
import numpy as np
import os
import argparse

def split_dataset(input_path, output_folder, num_files):
 
    df = pd.read_csv(input_path)

    os.makedirs(output_folder, exist_ok=True)

    chunks = np.array_split(df, num_files)

    for i, chunk in enumerate(chunks):
        file_path = os.path.join(output_folder, f"chunk_{i}.csv")
        chunk.to_csv(file_path, index=False)
        print(f"Saved {file_path} with {len(chunk)} rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into multiple files for ingestion job.")
    parser.add_argument("--input", type=str, required=True, help="Path to the dataset CSV")
    parser.add_argument("--output", type=str, default="raw-data", help="Output folder (default: raw-data)")
    parser.add_argument("--num_files", type=int, required=True, help="Number of files to generate")

    args = parser.parse_args()

    split_dataset(args.input, args.output, args.num_files)
