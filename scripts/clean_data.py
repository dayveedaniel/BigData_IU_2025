import pandas as pd
import numpy as np

# Read the CSV file
df = pd.read_csv('data/US_Accidents_March23.csv')

all_columns = [col for col in df.columns]
print(all_columns)

# Save the cleaned dataset
# df.to_csv('data/US_Accidents_March23_cleaned.csv', index=False)

# print("\nCleaning complete. Cleaned data saved to 'data/US_Accidents_March23_cleaned.csv'")
