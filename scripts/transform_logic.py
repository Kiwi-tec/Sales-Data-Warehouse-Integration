import pandas as pd
import numpy as np
import re

def standardize_cols(cols):
    return (
        pd.Index(cols)
        .map(str.strip)
        .map(str)
        .map(str.lower)
        .str.replace(r"\s+", "_", regex=True)
    )

def clean_sales_data(df):
    # 1. Standardize column names immediately
    df.columns = standardize_cols(df.columns)

    # 2. Handle duplicate columns by coalescing (Your original logic)
    groups = {}
    for f in df.columns:
        groups.setdefault(f, []).append(f)

    coalesced = {}
    for key, cols in groups.items():
        block = df[cols]
        # Replace common 'empty' strings with actual NaNs
        block = block.replace(["N/A", "n/a", "", "None"], np.nan)
        # bfill handles cases where data might be split across duplicate columns
        coalesced[key] = block.bfill(axis=1).iloc[:, 0]

    clean = pd.DataFrame(coalesced)

    # 3. Specific Field Cleaning
    if 'product' in clean.columns:
        clean['product'] = (
            clean['product']
            .str.strip()
            .astype(str)
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.title()
        )

    # Coerce numeric and date types
    if 'quantity' in clean.columns:
        clean['quantity'] = pd.to_numeric(clean['quantity'], errors='coerce')
    if 'price' in clean.columns:
        clean['price'] = pd.to_numeric(clean['price'], errors='coerce')
    if 'date' in clean.columns:
        clean['date'] = pd.to_datetime(clean['date'], errors='coerce')

    # 4. Data Quality Filtering
    # Drop rows where essential info is missing
    essential = [f for f in ['product', 'quantity', 'price', 'date'] if f in clean.columns]
    clean = clean.dropna(subset=essential)

    # Remove non-positive financial values
    if 'quantity' in clean.columns:
        clean = clean[clean['quantity'] > 0]
    if 'price' in clean.columns:
        clean = clean[clean['price'] > 0]

    # Remove duplicates
    clean = clean.drop_duplicates(subset=essential)

    # 5. Feature Engineering
    if {'quantity', 'price'}.issubset(clean.columns):
        clean['revenue'] = clean['quantity'] * clean['price']

    return clean