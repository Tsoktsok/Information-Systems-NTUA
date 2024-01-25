import argparse
import pandas as pd
import numpy as np

def generate_large_dataset(rows, features, output_file):

    chunk_size=10000

    for i in range(0, rows, chunk_size):
        chunk_rows = min(chunk_size, rows - i)
        data = np.random.randint(0, 10000, size=(chunk_rows, features))
        df = pd.DataFrame(data, columns=[f'feature_{j+1}' for j in range(features)])
        df.columns.values[-1] = 'label'
        df.to_csv(output_file, mode='a', header=not i, index=False)
    print(f"Generated {rows} rows with {features} features. Saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a large dataset in CSV format.")
    parser.add_argument("--rows", type=int, help="Number of rows in the dataset", required=True)
    parser.add_argument("--features", type=int, help="Number of features (columns) in the dataset", required=True)
    parser.add_argument("--output_file", type=str, help="Output CSV file name", required=True)

    args = parser.parse_args()

    generate_large_dataset(args.rows, args.features, args.output_file)