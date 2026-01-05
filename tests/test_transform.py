import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add the scripts folder to path so we can import the logic
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from transform_logic import clean_sales_data

class TestTransformLogic(unittest.TestCase):

    def setUp(self):
        # Create a sample raw dataframe matching your project's messy input style
        self.raw_data = pd.DataFrame({
            ' Product ': ['Laptop', 'Mouse', 'Keyboard'],
            'Quantity': [5, np.nan, 10],
            'Price': [1000, 20, -5],  # includes a negative price to test filtering
            'Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'quantity': [5, 2, 10]    # Duplicate column to test coalescing logic
        })

    def test_cleaning_and_revenue(self):
        # Run the transformation logic
        cleaned_df = clean_sales_data(self.raw_data)

        # 1. Test Column Standardization
        self.assertIn('product', cleaned_df.columns)
        self.assertIn('revenue', cleaned_df.columns)

        # 2. Test Data Quality Filtering
        # The row with Price -5 should have been dropped
        # The row with NaN quantity (Mouse) should be saved by the duplicate column coalesce
        self.assertEqual(len(cleaned_df), 2)

        # 3. Test Revenue Calculation
        # Laptop: 5 * 1000 = 5000
        laptop_revenue = cleaned_df[cleaned_df['product'] == 'Laptop']['revenue'].iloc[0]
        self.assertEqual(laptop_revenue, 5000)

if __name__ == '__main__':
    unittest.main()