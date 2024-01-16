import ray
#from ray.util.diagnostics import summarize
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
from measure_performance import measure_performance as mp

# Initialize Ray
ray.init()

# Load your CSV file into a Pandas DataFrame
csv_file_path = "data13.csv"
df = pd.read_csv(csv_file_path)

# Split the data into features and labels
X = df.iloc[:, :-1]  # Features
y = df.iloc[:, -1]   # Labels

# Create a train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1, random_state=42)

# Convert the Pandas DataFrames to Ray ObjectStores
X_train_id = ray.put(X_train)
y_train_id = ray.put(y_train)
X_test_id = ray.put(X_test)
y_test_id = ray.put(y_test)

# Define a function to train the RandomForestRegressor
@ray.remote
def train_random_forest(X_train, y_train):
    model = RandomForestRegressor()
    model.fit(X_train, y_train)
    return model

# Train the RandomForestRegressor remotely
random_forest_model_id = train_random_forest.remote(X_train_id, y_train_id)

# Fetch the trained model
random_forest_model = ray.get(random_forest_model_id)

# Predict on the test set
y_pred = random_forest_model.predict(X_test_id)

print(len(y_pred))

# Shutdown Ray
ray.shutdown()
