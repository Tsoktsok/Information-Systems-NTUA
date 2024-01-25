import argparse
import pandas as pd
import numpy as np

def generate_large_dataset(rows, features, output_file):
    # Create a DataFrame with fake data
    data = np.random.randint(0, 10000, size=(rows, features))
    df = pd.DataFrame(data, columns=[f'feature_{i+1}' for i in range(features)])
    df.columns.values[-1] = 'label'

    # Export to CSV
    df.to_csv(output_file, index=False)
    print(f"Generated {rows} rows with {features} features. Saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a large dataset in CSV format.")
    parser.add_argument("--rows", type=int, help="Number of rows in the dataset", required=True)
    parser.add_argument("--features", type=int, help="Number of features (columns) in the dataset", required=True)
    parser.add_argument("--output_file", type=str, help="Output CSV file name", required=True)

    args = parser.parse_args()

    generate_large_dataset(args.rows, args.features, args.output_file)